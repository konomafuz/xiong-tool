from flask import Flask, render_template, request, session, send_file, redirect, url_for, flash, jsonify
from modules import top_earners, smart_accounts, gmgn
from utils import fetch_data, export_to_excel
import time
import datetime
import pandas as pd
import json
# 新增的模块
from modules import holder, parse_transactions, estimate_costs, cluster_addresses
import os
from modules import wallet_tag_engine
import gc
import signal

# 数据库相关导入
from dotenv import load_dotenv
from config.database import init_database, get_db_config, cleanup_db_connections
from services.database_service import TopTraderService, WalletTagService, AnalysisJobService, with_long_running_session
import logging

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'your_secret_key_here')  # 从环境变量获取
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SESSION_TYPE'] = 'filesystem'  # 使用文件系统存储会话
app.config['SESSION_PERMANENT'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(hours=24)  # 会话有效期24小时

# 尝试使用Flask-Session扩展，如果已安装
try:
    from flask_session import Session
    Session(app)
    logger.info("🔐 已启用Flask-Session扩展，会话将更加稳定")
except ImportError:
    logger.warning("⚠️ 未安装Flask-Session扩展，使用默认会话存储")
    pass

# 数据库初始化
try:
    logger.info("🔄 开始数据库初始化...")
    db_config = init_database()
    logger.info("🎯 数据库连接成功！")
except Exception as e:
    logger.error(f"❌ 数据库连接失败: {e}")
    logger.warning("⚠️  应用将在无数据库模式下运行")
    db_config = None

# 🔧 免费版Render：应用启动时自动初始化数据库
def initialize_database_tables():
    """在应用启动时初始化数据库表（免费版Render适配）"""
    if not db_config:
        logger.warning("⚠️  数据库配置不可用，跳过表初始化")
        return False
    
    try:
        logger.info("📋 初始化数据库表...")
        from models.database_models import Base
        
        # 创建所有表
        Base.metadata.create_all(bind=db_config.get_engine())
        logger.info("✅ 数据库表初始化完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库表初始化失败: {e}")
        return False

# 执行数据库表初始化
initialize_database_tables()

@app.route("/")
def index():
    """首页"""
    return render_template("index.html")

# 将之前的debug_traders保留，但改为独立功能
@app.route("/debug_traders", methods=["GET", "POST"])
def debug_traders():
    """调试交易者数据结构"""
    if request.method == "POST":
        token_address = request.form.get("tokenAddress", "").strip()
        chain_id = request.form.get("chainId", "501").strip()
        
        try:
            from modules.top_earners import fetch_top_traders
            
            traders = fetch_top_traders(token_address, chain_id, 3)  # 只获取3个进行调试
            
            if traders:
                import json
                debug_info = {
                    'count': len(traders),
                    'sample_trader': traders[0],
                    'all_fields': list(traders[0].keys()) if traders else []
                }
                
                return f"<pre>{json.dumps(debug_info, indent=2, ensure_ascii=False)}</pre>"
            else:
                return "没有获取到交易者数据"
                
        except Exception as e:
            import traceback
            return f"<pre>错误: {e}\n\n{traceback.format_exc()}</pre>"
    
    return '''
    <form method="post">
        代币地址: <input type="text" name="tokenAddress" value="HtTYHz1Kf3rrQo6AqDLmss7gq5WrkWAaXn3tupUZbonk" style="width:400px">
        <br><br>链ID: <input type="text" name="chainId" value="501">
        <br><br><input type="submit" value="调试数据结构">
    </form>
    '''

# 恢复原来的top_earners路由
@app.route("/top_earners", methods=["GET", "POST"])
def top_earners_view():
    chain_id = request.args.get("chainId", request.form.get("chainId", ""))
    chain_id = chain_id.strip()
    token_address = request.args.get("tokenAddress", request.form.get("tokenAddress", ""))
    token_address = token_address.strip()
    limit = request.args.get("limit", request.form.get("limit", "100"))
    try:
        limit = int(limit)
    except Exception:
        limit = 100

    if request.method == "POST" and token_address and chain_id:
        try:
            # 优先使用数据库缓存（如果启用且数据新鲜）
            traders = []
            use_cache = os.getenv('ENABLE_DATABASE_CACHE', 'True').lower() == 'true'
            
            if use_cache and db_config:
                logger.info("🔍 检查数据库缓存...")
                if TopTraderService.is_data_fresh(token_address, chain_id, max_age_hours=1):
                    traders = TopTraderService.get_traders(token_address, chain_id, limit)
                    logger.info(f"✅ 使用数据库缓存，获取到 {len(traders)} 个交易者")
            
            # 如果缓存无数据，从API获取
            if not traders:
                logger.info("🌐 从API获取数据...")
                traders = top_earners.fetch_top_traders(token_address, chain_id=chain_id, limit=limit)
                
                # 保存到数据库（如果启用数据库）
                if traders and db_config:
                    try:
                        TopTraderService.save_traders(traders, token_address, chain_id)
                        logger.info("💾 数据已保存到数据库")
                    except Exception as e:
                        logger.warning(f"⚠️  保存到数据库失败: {e}")
            
            df = top_earners.prepare_traders_data(traders)
            
            # 保存完整数据到session
            session['traders_data'] = df.to_dict()
            session['token_address'] = token_address
            session['chain_id'] = chain_id
            session['limit'] = limit
            
            # 为显示准备数据
            if not df.empty:
                # 添加地址超链接
                if "walletAddress" in df.columns:
                    df["walletAddress"] = df["walletAddress"].apply(
                        lambda x: f'<a href="/address/{x}" title="{x}">{x[:5]}...{x[-5:]}</a>' if pd.notna(x) else ""
                    )
                elif "holderWalletAddress" in df.columns:
                    df["holderWalletAddress"] = df["holderWalletAddress"].apply(
                        lambda x: f'<a href="/address/{x}" title="{x}">{x[:5]}...{x[-5:]}</a>' if pd.notna(x) else ""
                    )
                
                # 添加浏览器链接
                if "explorerUrl" in df.columns:
                    df["explorerUrl"] = df["explorerUrl"].apply(
                        lambda x: f'<a href="{x}" target="_blank">查看</a>' if pd.notna(x) and x else ""
                    )
                
                # 格式化时间戳
                if "lastTradeTime" in df.columns:
                    df["lastTradeTime"] = pd.to_datetime(df["lastTradeTime"], unit='ms', errors='coerce')
                
                # 格式化数值
                numeric_columns = ['totalPnl', 'winRate', 'avgProfit', 'avgLoss', 'maxProfit', 'maxLoss']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                
                # 显示的关键字段 - 更新为实际可用的字段
                display_columns = [
                    "walletAddress", "totalPnl", "totalProfitPercentage", 
                    "realizedProfit", "realizedProfitPercentage",
                    "buyCount", "sellCount", "totalCount", 
                    "buyValue", "sellValue", "holdAmount",
                    "boughtAvgPrice", "soldAvgPrice",
                    "tags", "explorerUrl"
                ]
                
                # 过滤出存在的列
                display_df = df[[col for col in display_columns if col in df.columns]]
                
                table_html = display_df.to_html(
                    classes="table table-hover table-bordered table-striped", 
                    index=False, 
                    escape=False,
                    render_links=True
                ) if not display_df.empty else ""
            else:
                table_html = ""
            
            return render_template(
                "top_earners.html", 
                table=table_html, 
                token=token_address, 
                chain_id=chain_id,
                limit=limit,
                record_count=len(df)
            )
        except Exception as e:
            flash(f"查询失败: {str(e)}", "danger")
            return redirect(url_for('top_earners_view'))

    # session恢复逻辑保持不变...
    if 'traders_data' in session and session.get('token_address') == token_address and session.get('chain_id') == chain_id and session.get('limit', 100) == limit:
        df = pd.DataFrame(session['traders_data'])
        
        if not df.empty:
            # 重新应用格式化逻辑
            display_columns = [
                "walletAddress", "holderWalletAddress", "totalPnl", "winRate",
                "winCount", "lossCount", "totalCount", "avgProfit", "avgLoss",
                "maxProfit", "maxLoss", "roi", "tags"
            ]
            
            display_df = df[[col for col in display_columns if col in df.columns]]
            
            table_html = display_df.to_html(
                classes="table table-hover table-bordered table-striped", 
                index=False, 
                escape=False,
                render_links=True
            )
            return render_template(
                "top_earners.html", 
                table=table_html, 
                token=token_address, 
                chain_id=chain_id,
                limit=limit,
                record_count=len(df)
            )
    
    return render_template("top_earners.html", token=token_address, chain_id=chain_id, limit=limit)

@app.route("/download_top_earners", methods=["GET", "POST"])
def download_top_earners():
    if request.method != "POST":
        # 对于GET请求，直接返回404
        return "Not Found", 404

    token_address = request.form.get("tokenAddress", "").strip()
    chain_id = request.form.get("chainId", "").strip()
    limit = request.form.get("limit", "100").strip()
    try:
        limit = int(limit)
    except Exception:
        limit = 100

    if not token_address or not chain_id:
        flash("缺少代币地址或链ID", "danger")
        return redirect(url_for('top_earners_view'))

    try:
        if (
            'traders_data' in session and
            session.get('token_address') == token_address and
            session.get('chain_id') == chain_id and
            session.get('limit', 100) == limit
        ):
            import pandas as pd
            df = pd.DataFrame(session['traders_data'])
        else:
            traders = top_earners.fetch_top_traders(token_address, chain_id=chain_id, limit=limit)
            df = top_earners.prepare_traders_data(traders)
        return export_to_excel(df, f"top_traders_{token_address[:10]}")
    except Exception as e:
        flash(f"导出失败: {str(e)}", "danger")
        return redirect(url_for('top_earners_view'))

@app.route("/address/<wallet>")
def address_detail(wallet):
    try:
        tokens = top_earners.fetch_address_token_list(wallet)
        df = top_earners.prepare_tokens_data(tokens)
        
        # 保存完整数据到session
        session['address_tokens_data'] = df.to_dict()
        session['wallet_address'] = wallet
        
        # 添加地址超链接
        if not df.empty and "tokenContractAddress" in df.columns:
            df["tokenContractAddress"] = df["tokenContractAddress"].apply(
                lambda x: f'<a href="/top_earners?tokenAddress={x}" title="{x}">{x[:5]}...{x[-5:]}</a>'
            )
        
        # 添加浏览器链接
        if not df.empty and "explorerUrl" in df.columns:
            df["explorerUrl"] = df["explorerUrl"].apply(
                lambda x: f'<a href="{x}" target="_blank">查看</a>' if x else ""
            )
        elif not df.empty and "innerGotoUrl" in df.columns:
            df["innerGotoUrl"] = df["innerGotoUrl"].apply(
                lambda x: f'<a href="{x}" target="_blank">查看</a>' if x else ""
            )
        
        # 格式化时间戳
        if not df.empty and "latestTime" in df.columns:
            df["latestTime"] = pd.to_datetime(df["latestTime"], unit='ms')
        
        # 只显示部分关键字段
        display_columns = [
            "tokenSymbol", "tokenContractAddress", "totalPnl", "totalPnlPercentage",
            "buyVolume", "sellVolume", "balance", "balanceUsd", 
            "latestTime", "explorerUrl", "innerGotoUrl"
        ]
        
        # 过滤出存在的列
        display_df = df[[col for col in display_columns if col in df.columns]]
        
        table_html = display_df.to_html(
            classes="table table-hover table-bordered table-striped", 
            index=False, 
            escape=False,
            render_links=True
        ) if not display_df.empty else ""
        
        return render_template(
            "address_detail.html", 
            wallet=wallet, 
            table=table_html, 
            error=None,
            record_count=len(df)
        )
    except Exception as e:
        return render_template("address_detail.html", wallet=wallet, table=None, error=str(e))

@app.route("/download_address_tokens", methods=["POST"])
def download_address_tokens():
    wallet_address = request.form.get("walletAddress", "").strip()
    if not wallet_address:
        flash("缺少钱包地址", "danger")
        return redirect(url_for('address_detail', wallet=wallet_address))
    
    try:
        # 优先使用session中的数据
        if 'address_tokens_data' in session and session.get('wallet_address') == wallet_address:
            df = pd.DataFrame(session['address_tokens_data'])
        else:
            tokens = top_earners.fetch_address_token_list(wallet_address)
            df = top_earners.prepare_tokens_data(tokens)
        
        return export_to_excel(df, f"top_tokens_{wallet_address[:10]}")
    except Exception as e:
        flash(f"导出失败: {str(e)}", "danger")
        return redirect(url_for('address_detail', wallet=wallet_address))

@app.route("/smart_accounts", methods=["GET", "POST"])
def smart_accounts():
    if request.method == "POST":
        target_address = request.form.get("targetAddress", "").strip()
        chain_id = request.form.get("chainId", "501").strip()
        max_tokens = int(request.form.get("maxTokens", 50))
        page_limit = int(request.form.get("pageLimit", 5))
        
        if not target_address:
            flash("请输入目标地址", "danger")
            return render_template("smart_accounts.html")
        
        try:
            start_time = time.time()
            results = smart_accounts.find_smart_accounts(target_address, chain_id, max_tokens, page_limit)
            elapsed_time = time.time() - start_time
            
            # 保存结果到session
            session['smart_accounts_results'] = results
            session['smart_accounts_params'] = {
                'targetAddress': target_address,
                'chainId': chain_id,
                'maxTokens': max_tokens,
                'pageLimit': page_limit
            }
            
            return render_template(
                "smart_accounts.html",
                results=results,
                target_address=target_address,
                elapsed_time=round(elapsed_time, 2),
                record_count=len(results)
            )
        except Exception as e:
            flash(f"分析失败: {str(e)}", "danger")
            return render_template("smart_accounts.html")
    
    # 恢复上一次的结果
    if 'smart_accounts_results' in session:
        return render_template(
            "smart_accounts.html",
            results=session['smart_accounts_results'],
            target_address=session['smart_accounts_params']['targetAddress'],
            elapsed_time=0,
            record_count=len(session['smart_accounts_results'])
        )
    
    return render_template("smart_accounts.html")

@app.route("/download_smart_accounts", methods=["POST"])
def download_smart_accounts():
    if 'smart_accounts_results' not in session:
        flash("没有可导出的数据", "warning")
        return redirect(url_for('smart_accounts_view'))
    
    try:
        # 创建DataFrame
        data = []
        for address, count in session['smart_accounts_results']:
            data.append({
                "地址": address,
                "出现次数": count,
                "可疑度": "非常高" if count > 10 else "高" if count > 5 else "中" if count > 2 else "低"
            })
        df = pd.DataFrame(data)
        filename_prefix = f"smart_accounts_{session['smart_accounts_params']['targetAddress'][:10]}"
        return export_to_excel(df, filename_prefix)
    except Exception as e:
        flash(f"导出失败: {str(e)}", "danger")
        return redirect(url_for('smart_accounts'))

def merge_existing_remarks(normal_remarks, conspiracy_remarks, existing_remarks_text, merge_strategy):
    """
    合并已有备注地址和新生成的备注
    
    Args:
        normal_remarks: 新生成的普通地址备注列表
        conspiracy_remarks: 新生成的阴谋钱包备注列表
        existing_remarks_text: 已有备注地址文本，支持多种格式：
                              1. "地址:备注" 每行一个
                              2. JSON格式: [{"address": "xxx", "name": "xxx"}]
        merge_strategy: 合并策略，"keep_existing" 或 "keep_new"
    
    Returns:
        tuple: (合并后的normal_remarks, 合并后的conspiracy_remarks, 冲突列表)
    """
    conflicts = []
    existing_remarks = {}
    
    # 解析已有备注地址 - 支持多种格式
    existing_remarks_text = existing_remarks_text.strip()
    
    # 检查是否为JSON格式
    if existing_remarks_text.startswith('[') and existing_remarks_text.endswith(']'):
        try:
            # JSON格式解析
            import json
            json_data = json.loads(existing_remarks_text)
            for item in json_data:
                if isinstance(item, dict) and 'address' in item:
                    address = item['address'].strip().lower()
                    # 支持多种备注字段名
                    remark = item.get('name') or item.get('remark') or item.get('label') or ''
                    if address and remark:
                        existing_remarks[address] = remark
        except json.JSONDecodeError:
            # JSON解析失败，按普通文本处理
            pass
    
    # 如果不是JSON或JSON解析失败，按行解析
    if not existing_remarks:
        for line in existing_remarks_text.split('\n'):
            line = line.strip()
            if not line or ':' not in line:
                continue
            
            try:
                address, remark = line.split(':', 1)
                address = address.strip().lower()  # 统一转换为小写
                remark = remark.strip()
                if address and remark:
                    existing_remarks[address] = remark
            except ValueError:
                continue
    
    # 创建新备注的地址映射
    new_normal = {item['address'].lower(): item for item in normal_remarks}
    new_conspiracy = {item['address'].lower(): item for item in conspiracy_remarks}
    
    # 合并逻辑
    merged_normal = []
    merged_conspiracy = []
    processed_addresses = set()
    
    # 处理已有备注
    for address, existing_remark in existing_remarks.items():
        processed_addresses.add(address)
        
        # 检查是否在新备注中有冲突
        conflict_item = None
        new_remark = None
        is_conspiracy = False
        
        if address in new_normal:
            conflict_item = new_normal[address]
            new_remark = conflict_item['remark']
        elif address in new_conspiracy:
            conflict_item = new_conspiracy[address]
            new_remark = conflict_item['remark']
            is_conspiracy = True
        
        if conflict_item and new_remark != existing_remark:
            # 有冲突，记录冲突信息让用户手动选择
            conflicts.append({
                'address': conflict_item['address'],  # 保持原始大小写
                'existing_remark': existing_remark,
                'new_remark': new_remark,
                'is_conspiracy': is_conspiracy
            })
            
            # 冲突的地址暂时不添加到结果中，等用户选择后再处理
            # 这样确保用户可以手动选择每个冲突地址的备注
                
        elif conflict_item:
            # 没有冲突，备注相同，直接使用
            if is_conspiracy:
                merged_conspiracy.append(conflict_item)
            else:
                merged_normal.append(conflict_item)
        else:
            # 已有备注但新备注中没有这个地址，直接添加到普通备注
            merged_normal.append({
                'address': address.upper(),  # 恢复原始格式
                'remark': existing_remark
            })
    
    # 添加新备注中没有冲突的地址
    for address, item in new_normal.items():
        if address not in processed_addresses:
            merged_normal.append(item)
    
    for address, item in new_conspiracy.items():
        if address not in processed_addresses:
            merged_conspiracy.append(item)
    
    return merged_normal, merged_conspiracy, conflicts

@app.route("/download_gmgn_remarks", methods=["POST"])
def download_gmgn_remarks():
    """下载GMGN备注数据"""
    if 'gmgn_results' not in session:
        flash("没有可导出的数据", "warning")
        return redirect(url_for('gmgn_tool'))
    
    remark_type = request.form.get("remarkType", "all")  # all, normal, conspiracy
    export_format = request.form.get("exportFormat", "excel")  # excel, txt
    
    try:
        results = session['gmgn_results']
        normal_remarks = results.get('normal_remarks', [])
        conspiracy_remarks = results.get('conspiracy_remarks', [])
        params = results.get('params', {})
        
        # 根据类型准备数据
        data_to_export = []
        
        if remark_type == "all":
            data_to_export.extend(normal_remarks)
            data_to_export.extend(conspiracy_remarks)
        elif remark_type == "normal":
            data_to_export = normal_remarks
        elif remark_type == "conspiracy":
            data_to_export = conspiracy_remarks
        
        if not data_to_export:
            flash("没有数据可导出", "warning")
            return redirect(url_for('gmgn_tool'))
        
        # 创建DataFrame
        df = pd.DataFrame(data_to_export)
        df.index = df.index + 1  # 从1开始编号
        
        # 添加元数据
        ca_name = params.get('ca_name', 'Unknown')
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        
        if export_format == "txt":
            # 导出为文本格式 (地址:备注)
            filename = f"gmgn_remarks_{ca_name}_{remark_type}_{timestamp}.txt"
            content = "\n".join([f"{item['address']}:{item['remark']}" for item in data_to_export])
            
            import io
            output = io.StringIO()
            output.write(content)
            output.seek(0)
            
            from flask import Response
            return Response(
                output.getvalue(),
                mimetype="text/plain",
                headers={"Content-disposition": f"attachment; filename={filename}"}
            )
        else:
            # 导出为Excel格式
            filename_prefix = f"gmgn_remarks_{ca_name}_{remark_type}_{timestamp}"
            return export_to_excel(df, filename_prefix)
            
    except Exception as e:
        flash(f"导出失败: {str(e)}", "danger")
        return redirect(url_for('gmgn_tool'))

@app.route("/gmgn", methods=["GET", "POST"])
def gmgn_tool():
    normal_remarks = None
    conspiracy_remarks = None
    
    if request.method == "POST":
        ca_address = request.form.get("caAddress", "").strip()
        ca_name = request.form.get("caName", "").strip()
        chain_id = request.form.get("chainId", "501").strip()
        
        # 获取数量参数
        holder_count = request.form.get("holderCount", "50")
        trader_count = request.form.get("traderCount", "50")
        
        # 阴谋钱包检测参数
        conspiracy_check = request.form.get("conspiracyCheck") == "on"
        conspiracy_days = request.form.get("conspiracyDays", "10")
        
        # 已有备注地址和合并策略
        existing_remarks_text = request.form.get("existingRemarks", "").strip()
        merge_strategy = request.form.get("mergeStrategy", "keep_existing")
        
        try:
            holder_count = max(1, min(200, int(holder_count)))
        except ValueError:
            holder_count = 50
            
        try:
            trader_count = max(1, min(200, int(trader_count)))
        except ValueError:
            trader_count = 50
            
        try:
            conspiracy_days = max(1, min(30, int(conspiracy_days)))
        except ValueError:
            conspiracy_days = 10
        
        if not ca_address or not ca_name:
            flash("请输入CA地址以及名称", "danger")
            return render_template("gmgn.html")
            
        try:
            print(f"🎯 开始获取备注数据...")
            print(f"📊 Holders数量: {holder_count}, Traders数量: {trader_count}")
            print(f"🔍 阴谋钱包检测: {'启用' if conspiracy_check else '关闭'}")
            
            # 使用新的生成函数，支持多链
            result = gmgn.generate_address_remarks(
                ca_address, 
                ca_name, 
                holder_count, 
                trader_count,
                conspiracy_check,
                conspiracy_days,
                chain_id  # 🔧 传递链ID参数
            )
            
            normal_remarks = result.get("normal_remarks", [])
            conspiracy_remarks = result.get("conspiracy_remarks", [])
            
            # 处理已有备注地址的合并
            if existing_remarks_text:
                normal_remarks, conspiracy_remarks, conflicts = merge_existing_remarks(
                    normal_remarks, conspiracy_remarks, existing_remarks_text, merge_strategy
                )
                
                if conflicts:
                    # 如果有冲突，将冲突信息传递给前端
                    session['address_conflicts'] = conflicts
                    flash(f"发现 {len(conflicts)} 个地址备注冲突，请手动选择", "warning")
            
            print(f"🎉 备注数据生成完成!")
            print(f"📊 普通地址: {len(normal_remarks)} 个")
            print(f"🐟 阴谋钱包: {len(conspiracy_remarks)} 个")
            
            # 保存结果到session
            session['gmgn_results'] = {
                'normal_remarks': normal_remarks,
                'conspiracy_remarks': conspiracy_remarks,
                'params': {
                    'ca_address': ca_address,
                    'ca_name': ca_name,
                    'chain_id': chain_id,  # 🔧 保存链ID
                    'holder_count': holder_count,
                    'trader_count': trader_count,
                    'conspiracy_check': conspiracy_check,
                    'conspiracy_days': conspiracy_days
                }
            }
            
            if conspiracy_check:
                flash(f"分析完成！普通地址 {len(normal_remarks)} 个，阴谋钱包 {len(conspiracy_remarks)} 个", "success")
            else:
                flash(f"分析完成！共生成 {len(normal_remarks)} 个地址备注", "success")
            
        except Exception as e:
            print(f"❌ 查询失败: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f"查询失败: {str(e)}", "danger")
    
    return render_template("gmgn.html", 
                         normal_remarks=normal_remarks,
                         conspiracy_remarks=conspiracy_remarks)

@app.route("/resolve_conflicts", methods=["POST"])
def resolve_conflicts():
    """处理地址备注冲突"""
    if 'address_conflicts' not in session or 'gmgn_results' not in session:
        flash("没有待处理的冲突", "warning")
        return redirect(url_for('gmgn_tool'))
    
    try:
        conflicts = session['address_conflicts']
        results = session['gmgn_results']
        normal_remarks = results.get('normal_remarks', [])
        conspiracy_remarks = results.get('conspiracy_remarks', [])
        
        # 根据用户选择处理冲突地址
        for i, conflict in enumerate(conflicts):
            choice = request.form.get(f"conflict_{i}")
            if choice == "existing":
                final_remark = conflict['existing_remark']
            else:  # choice == "new"
                final_remark = conflict['new_remark']
            
            # 创建解决后的地址项
            resolved_item = {
                'address': conflict['address'],
                'remark': final_remark
            }
            
            # 添加到相应列表
            if conflict['is_conspiracy']:
                conspiracy_remarks.append(resolved_item)
            else:
                normal_remarks.append(resolved_item)
        
        # 更新session中的结果
        session['gmgn_results']['normal_remarks'] = normal_remarks
        session['gmgn_results']['conspiracy_remarks'] = conspiracy_remarks
        
        # 清除冲突信息
        if 'address_conflicts' in session:
            del session['address_conflicts']
        
        flash("冲突已解决", "success")
        
    except Exception as e:
        flash(f"处理冲突时出错: {str(e)}", "danger")
    
    return redirect(url_for('gmgn_tool'))

@app.route("/ignore_conflicts", methods=["POST"])
def ignore_conflicts():
    """忽略地址备注冲突"""
    if 'address_conflicts' in session:
        del session['address_conflicts']
        flash("已忽略冲突", "info")
    return redirect(url_for('gmgn_tool'))

# 新增的路由
@app.route("/solana_analysis", methods=["GET", "POST"])
def solana_analysis():
    if request.method == "POST":
        token_address = request.form.get("tokenAddress", "").strip()
        chain_id = request.form.get("chainId", "501").strip()
        top_n = int(request.form.get("topN", 50))
        helius_api_key = request.form.get("heliusApiKey", "").strip()
        token_id = request.form.get("tokenId", "").strip()  # CoinGecko token ID
        
        if not token_address:
            flash("请输入Token地址", "danger")
            return render_template("solana_analysis.html")
        
        try:
            # 步骤1: 获取Top Holders
            print("正在获取Top Holders...")
            timestamp = int(time.time() * 1000)
            holders_df = holder.get_all_holders(chain_id, token_address, timestamp, top_n)
            
            if holders_df.empty:
                flash("未找到持仓数据", "warning")
                return render_template("solana_analysis.html")
            
            # 保存基础数据
            session['solana_holders'] = holders_df.to_dict()
            session['solana_params'] = {
                'tokenAddress': token_address,
                'chainId': chain_id,
                'topN': top_n,
                'tokenId': token_id
            }
            
            results = {
                'holders_count': len(holders_df),
                'total_percentage': holders_df['percentage'].sum()
            }
            
            # 步骤2: 解析交易（如果提供了Helius API密钥）
            if helius_api_key:
                print("正在解析交易...")
                events_df = parse_transactions.batch_analyze_holders(
                    holders_df, helius_api_key, token_address
                )
                
                if not events_df.empty:
                    session['solana_events'] = events_df.to_dict()
                    results['transactions_count'] = len(events_df)
                    results['unique_addresses'] = events_df['address'].nunique()
                    
                    # 步骤3: 成本估算（如果提供了CoinGecko token ID）
                    if token_id:
                        print("正在估算成本...")
                        pnl_df = estimate_costs.analyze_holder_profitability(
                            holders_df, events_df, token_id
                        )
                        
                        if not pnl_df.empty:
                            session['solana_pnl'] = pnl_df.to_dict()
                            results['avg_multiplier'] = pnl_df['multiplier'].mean()
                            results['profitable_addresses'] = (pnl_df['unrealized_pnl_usd'] > 0).sum()
                    
                    # 步骤4: 聚类分析
                    print("正在进行聚类分析...")
                    output_dir = os.path.join('static', 'temp')
                    cluster_results = cluster_addresses.full_cluster_analysis(
                        events_df, holders_df, output_dir
                    )
                    
                    if cluster_results:
                        session['solana_clusters'] = {
                            k: v.to_dict() if hasattr(v, 'to_dict') else v 
                            for k, v in cluster_results.items()
                        }
                        
                        if 'transfer_stats' in cluster_results:
                            results['cluster_count'] = len(cluster_results['transfer_stats'])
            
            return render_template("solana_analysis.html", results=results)
            
        except Exception as e:
            flash(f"分析失败: {str(e)}", "danger")
            return render_template("solana_analysis.html")
    
    return render_template("solana_analysis.html")

@app.route("/download_solana_data/<data_type>")
def download_solana_data(data_type):
    """下载Solana分析数据"""
    try:
        if data_type == 'holders' and 'solana_holders' in session:
            df = pd.DataFrame(session['solana_holders'])
            return export_to_excel(df, f"solana_holders_{int(time.time())}")
        
        elif data_type == 'events' and 'solana_events' in session:
            df = pd.DataFrame(session['solana_events'])
            return export_to_excel(df, f"solana_events_{int(time.time())}")
        
        elif data_type == 'pnl' and 'solana_pnl' in session:
            df = pd.DataFrame(session['solana_pnl'])
            return export_to_excel(df, f"solana_pnl_{int(time.time())}")
        
        elif data_type == 'clusters' and 'solana_clusters' in session:
            cluster_data = session['solana_clusters']
            if 'transfer_clusters' in cluster_data:
                df = pd.DataFrame(cluster_data['transfer_clusters'])
                return export_to_excel(df, f"solana_clusters_{int(time.time())}")
        
        flash("数据不存在或已过期", "warning")
        return redirect(url_for('solana_analysis'))
        
    except Exception as e:
        flash(f"下载失败: {str(e)}", "danger")
        return redirect(url_for('solana_analysis'))

@app.route("/holder_snapshots", methods=["GET", "POST"])
def holder_snapshots():
    """基于定时采集数据的持仓快照分析"""
    from modules.holder import list_collection_tasks, analyze_holder_patterns
    
    # 获取所有采集任务
    tasks = list_collection_tasks()
    
    if request.method == "POST":
        task_id = request.form.get("task_id")
        top_n = int(request.form.get("top_n", 100))
        min_snapshots = int(request.form.get("min_snapshots", 3))
        
        if not task_id:
            flash("请选择一个采集任务", "danger")
            return render_template("holder_snapshots.html", tasks=tasks)
        
        try:
            # 分析持仓模式
            analysis_result = analyze_holder_patterns(task_id, top_n, min_snapshots)
            
            return render_template(
                "holder_snapshots.html",
                tasks=tasks,
                analysis_result=analysis_result,
                task_id=task_id,
                top_n=top_n,
                min_snapshots=min_snapshots
            )
            
        except Exception as e:
            flash(f"分析失败: {e}", "danger")
            return render_template("holder_snapshots.html", tasks=tasks)
    
    return render_template("holder_snapshots.html", tasks=tasks)

def cleanup_old_snapshot_files():
    """清理过期的快照临时文件"""
    import tempfile
    import os
    import glob
    import time
    
    # 获取临时目录
    temp_dir = os.path.join(tempfile.gettempdir(), 'okx_pnl_tool')
    
    # 确保目录存在
    if not os.path.exists(temp_dir):
        return
        
    # 寻找所有快照文件
    snapshot_files = glob.glob(os.path.join(temp_dir, 'snapshot_*.pkl'))
    
    # 检查每个文件
    now = time.time()
    for file_path in snapshot_files:
        # 如果文件超过12小时未修改，则删除
        if os.path.exists(file_path) and (now - os.path.getmtime(file_path)) > 12 * 3600:
            try:
                os.remove(file_path)
                logger.info(f"已清理过期快照文件: {file_path}")
            except Exception as e:
                logger.error(f"清理快照文件失败: {file_path}, 错误: {str(e)}")

@app.route('/download_holder_snapshots', methods=["POST"])
def download_holder_snapshots():
    """下载持仓快照数据"""
    # 清理老旧文件
    cleanup_old_snapshot_files()
    if 'holder_snapshots' not in session:
        flash("没有可导出的快照数据", "warning")
        return redirect(url_for('holder_snapshots'))
    
    try:
        # 获取临时文件路径与当前请求的代币地址
        snapshot_info = session['holder_snapshots']
        temp_file = snapshot_info.get('temp_file')
        session_token_address = snapshot_info.get('token_address')
        current_token_address = request.form.get('tokenAddress')  # 从表单获取当前查询的代币地址
        
        # 日志记录
        if current_token_address:
            logger.info(f"当前请求导出代币: {current_token_address}")
            logger.info(f"Session中保存的代币: {session_token_address}")
            
            # 如果表单中提供了代币地址，且与session中不一致，说明用户已经查询了新的代币
            if session_token_address != current_token_address:
                flash(f"检测到代币地址变更，请先查询新代币的快照数据后再导出", "warning")
                return redirect(url_for('holder_snapshots'))
        
        if not temp_file or not os.path.exists(temp_file):
            flash("快照数据已过期或不存在，请重新生成", "warning")
            return redirect(url_for('holder_snapshots'))
        
        # 从临时文件加载数据
        import pickle
        with open(temp_file, 'rb') as f:
            try:
                snapshot_data = pickle.load(f)
                historical_data = snapshot_data['data']
                token_address = snapshot_data['token_address']
            except Exception as e:
                flash(f"无法加载快照数据: {str(e)}", "danger")
                return redirect(url_for('holder_snapshots'))
        
        export_type = request.form.get("exportType", "merged")  # merged, timeseries, all
        
        # 调用导出函数
        from modules.holder import export_holder_snapshots
                # 添加更多日志
        logger.info(f"正在导出快照数据，类型: {export_type}, 快照数量: {len(historical_data)}")
        logger.info(f"导出代币地址: {token_address}")
        
        # 对每个快照时间点记录数据量
        for label, df in historical_data.items():
            if isinstance(df, pd.DataFrame):
                logger.info(f"时间点 {label}: {len(df)} 条记录")
            else:
                logger.info(f"时间点 {label}: 非DataFrame类型 ({type(df)})")
        
        # 调用导出函数
        merged_path, timeseries_path = export_holder_snapshots(historical_data, token_address)        # 记录导出结果
        if merged_path:
            logger.info(f"合并CSV导出成功: {merged_path}")
        else:
            logger.warning("合并CSV导出失败")
            
        if timeseries_path:
            logger.info(f"时序CSV导出成功: {timeseries_path}")
        else:
            logger.warning("时序CSV导出失败")
        
        if export_type == "timeseries" and timeseries_path:
            # 返回时间序列格式
            try:
                return send_file(
                    timeseries_path,
                    as_attachment=True,
                    download_name=os.path.basename(timeseries_path),
                    mimetype="text/csv"
                )
            except Exception as e:
                logger.error(f"文件发送失败: {str(e)}")
                flash(f"文件下载失败: {str(e)}", "danger")
                return redirect(url_for('holder_snapshots'))
        elif export_type == "all":
            # 打包两个文件
            import zipfile
            from io import BytesIO
            
            memory_file = BytesIO()
            with zipfile.ZipFile(memory_file, 'w') as zf:
                if merged_path:
                    zf.write(merged_path, os.path.basename(merged_path))
                if timeseries_path:
                    zf.write(timeseries_path, os.path.basename(timeseries_path))
            
            memory_file.seek(0)
            return send_file(
                memory_file,
                as_attachment=True,
                download_name=f"holder_snapshots_{token_address[:8]}_{int(time.time())}.zip",
                mimetype="application/zip"
            )
        else:
            # 默认返回合并格式
            if merged_path:
                try:
                    return send_file(
                        merged_path,
                        as_attachment=True,
                        download_name=os.path.basename(merged_path),
                        mimetype="text/csv"
                    )
                except Exception as e:
                    logger.error(f"文件发送失败: {str(e)}")
                    flash(f"文件下载失败: {str(e)}", "danger")
                    return redirect(url_for('holder_snapshots'))
        
        flash("导出失败: 未生成可下载的文件", "danger")
        logger.error("导出失败: 未生成可下载的文件")
        return redirect(url_for('holder_snapshots'))
        
    except Exception as e:
        flash(f"导出失败: {str(e)}", "danger")
        return redirect(url_for('holder_snapshots'))

@app.route("/whale_flow")
def whale_flow():
    """庄家资金流动 - 待开发"""
    return render_template("coming_soon.html", 
                         feature_name="庄家资金流动",
                         description="追踪大户资金进出，分析市场操控行为和资金流向",
                         expected_features=[
                             "大额转账实时监控",
                             "资金流向可视化",
                             "异常交易预警",
                             "庄家行为分析"
                         ])

@app.route("/address_monitor")
def address_monitor():
    """地址实时监控 - 待开发"""
    return render_template("coming_soon.html",
                         feature_name="地址实时监控", 
                         description="实时监控重要地址的交易活动，及时获取投资信号",
                         expected_features=[
                             "多地址批量监控",
                             "交易实时推送",
                             "自定义监控规则",
                             "历史行为分析"
                         ])

@app.route("/wallet_analyzer", methods=["GET", "POST"])
def wallet_analyzer():
    """钱包智能分析器 - 支持批量地址和备注处理"""
    if request.method == "POST":
        wallet_addresses_text = request.form.get("walletAddresses", "").strip()
        chain_id = request.form.get("chainId", "501")
        preserve_remarks = request.form.get("preserveRemarks", "append")  # append, prepend, replace, ignore
        
        if not wallet_addresses_text:
            flash("请输入钱包地址", "danger")
            return render_template("wallet_analyzer.html")
        
        # 🔧 解析地址列表（支持多种格式）
        addresses_with_remarks = []
        
        # 支持逗号分隔或换行分隔
        if ',' in wallet_addresses_text and '\n' not in wallet_addresses_text:
            # 逗号分隔格式：0x...:备注,0x...:备注
            entries = wallet_addresses_text.split(',')
        else:
            # 换行分隔格式
            entries = wallet_addresses_text.split('\n')
        
        for entry in entries:
            entry = entry.strip()
            if not entry or len(entry) < 20:
                continue
                
            # 解析格式：地址:备注 或 纯地址
            if ':' in entry:
                parts = entry.split(':', 1)
                address = parts[0].strip()
                original_remark = parts[1].strip() if len(parts) > 1 else ""
            else:
                address = entry.strip()
                original_remark = ""
            
            # 基本地址验证（ETH和Solana地址长度检查）
            if len(address) >= 32:  # 支持ETH(42)和Solana(32-44)地址
                addresses_with_remarks.append({
                    'address': address,
                    'original_remark': original_remark
                })
        
        if not addresses_with_remarks:
            flash("未找到有效的钱包地址", "danger")
            return render_template("wallet_analyzer.html")
        
        # 🔧 限制最多100个地址
        if len(addresses_with_remarks) > 100:
            flash(f"最多支持100个地址同时分析，已截取前100个", "warning")
            addresses_with_remarks = addresses_with_remarks[:100]
        
        try:
            # 使用标签引擎
            engine = wallet_tag_engine.WalletTagEngine()
            
            # 只提取地址进行分析
            addresses = [item['address'] for item in addresses_with_remarks]
            print(f"🔍 开始批量分析 {len(addresses)} 个钱包...")
            results = engine.batch_analyze(addresses, chain_id)
            
            # 🔧 处理原始备注和新生成的标签
            for i, result in enumerate(results):
                if i < len(addresses_with_remarks):
                    original_remark = addresses_with_remarks[i]['original_remark']
                    generated_tags = result.get('tags', '')
                    
                    # 根据用户选择处理备注
                    if preserve_remarks == "replace":
                        # 覆盖：只使用生成的标签
                        final_remark = generated_tags
                    elif preserve_remarks == "prepend":
                        # 前面：原始备注 + 生成的标签
                        if original_remark and generated_tags:
                            final_remark = f"{original_remark} | {generated_tags}"
                        else:
                            final_remark = original_remark or generated_tags
                    elif preserve_remarks == "ignore":
                        # 忽略：只保留原始备注
                        final_remark = original_remark
                    else:  # append (默认)
                        # 后面：生成的标签 + 原始备注
                        if generated_tags and original_remark:
                            final_remark = f"{generated_tags} | {original_remark}"
                        else:
                            final_remark = generated_tags or original_remark
                    
                    result['original_remark'] = original_remark
                    result['final_remark'] = final_remark
                    result['generated_tags'] = generated_tags
                else:
                    result['original_remark'] = ""
                    result['final_remark'] = result.get('tags', '')
                    result['generated_tags'] = result.get('tags', '')
            
            # 保存结果到session
            session['wallet_analyzer_results'] = results
            session['wallet_analyzer_params'] = {
                'addresses': addresses,
                'chain_id': chain_id,
                'preserve_remarks': preserve_remarks,
                'total_count': len(addresses)
            }
            
            flash(f"分析完成！成功分析了 {len(results)} 个钱包", "success")
            
            return render_template("wallet_analyzer.html", 
                                 results=results,
                                 chain_id=chain_id,
                                 wallet_addresses=wallet_addresses_text,
                                 preserve_remarks=preserve_remarks)
            
        except Exception as e:
            print(f"❌ 分析异常: {e}")
            import traceback
            print(f"📝 异常详情: {traceback.format_exc()}")
            
            # 🔧 如果是数据库连接问题，清理连接池
            if e and "connection" in str(e).lower():
                cleanup_db_connections()
                
            flash(f"分析失败: {str(e)}", "danger")
            return render_template("wallet_analyzer.html")
    
    return render_template("wallet_analyzer.html")

@app.route("/smart_wallet", methods=["GET", "POST"])
def smart_wallet():
    """智能钱包分析 - 重定向到新分析器"""
    return redirect(url_for('wallet_analyzer'))

@app.route('/get_top_profit', methods=['POST'])
def get_top_profit():
    """获取 TOP 盈利地址 - 移除数量限制"""
    try:
        data = request.get_json()
        token_address = data.get('token_address', '').strip()
        chain_id = data.get('chain_id', '501')
        # 移除数量限制，允许用户选择的值
        limit = int(data.get('limit', 50))
        
        # 只对极端值做合理限制
        if limit > 1000:
            limit = 1000
        elif limit < 1:
            limit = 50
        
        if not token_address:
            return jsonify({'error': '代币地址不能为空'}), 400
        
        print(f"🔍 开始查询代币 {token_address[:8]}... 的盈利地址，数量: {limit}")
        
        # 根据查询数量调整超时时间
        if limit <= 50:
            timeout_seconds = 22
        elif limit <= 100:
            timeout_seconds = 28
        elif limit <= 200:
            timeout_seconds = 35
        else:
            timeout_seconds = 45  # 大量查询需要更长时间
        
        # 设置动态超时保护
        def timeout_handler(signum, frame):
            raise TimeoutError("查询超时")
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)
        
        try:
            # 使用用户选择的数量
            traders = fetch_top_traders(token_address, chain_id, limit)
            
            if not traders:
                signal.alarm(0)
                return jsonify({
                    'error': '未找到该代币的盈利地址数据',
                    'success': False
                }), 404
            
            # 处理数据
            df = prepare_traders_data(traders)
            
            if df.empty:
                signal.alarm(0)
                return jsonify({
                    'error': '数据处理失败',
                    'success': False
                }), 500
            
            # 转换为JSON格式
            result_data = []
            for _, row in df.head(limit).iterrows():
                result_data.append({
                    'address': row['walletAddress'],
                    'pnl': float(row['totalPnl']),
                    'roi': float(row['roi']),
                    'buy_count': int(row['buyCount']),
                    'sell_count': int(row['sellCount']),
                    'win_rate': float(row['winRate']),
                    'tags': row['tags'],
                    'rank': int(row['rank'])
                })
            
            signal.alarm(0)
            
            # 手动垃圾回收
            del traders, df
            gc.collect()
            
            return jsonify({
                'success': True,
                'data': result_data,
                'total': len(result_data),
                'message': f'成功获取 {len(result_data)} 个盈利地址'
            })
            
        except TimeoutError:
            signal.alarm(0)
            return jsonify({
                'error': f'查询超时（{timeout_seconds}秒），请尝试减少查询数量',
                'success': False
            }), 408
            
    except Exception as e:
        print(f"❌ 查询盈利地址失败: {e}")
        return jsonify({
            'error': f'查询失败: {str(e)}',
            'success': False
        }), 500
    finally:
        # 确保清理内存
        gc.collect()

# 添加系统监控路由
@app.route('/system_status')
def system_status():
    """系统状态监控 - 包含连接池状态"""
    try:
        import psutil
        import os
        from services.database_service import get_connection_pool_status
        
        # 获取内存使用情况
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # 数据库连接测试
        db_status = "connected" if db_config and db_config.test_connection() else "disconnected"
        
        # 🔧 获取连接池状态
        pool_status = get_connection_pool_status() if db_config else {}
        
        return jsonify({
            'memory_usage_mb': memory_info.rss / 1024 / 1024,
            'memory_percent': process.memory_percent(),
            'database_status': db_status,
            'connection_pool': pool_status,  # 新增连接池信息
            'status': 'healthy'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        })

@app.route('/db_test')
def db_test():
    """数据库连接测试"""
    if not db_config:
        return jsonify({
            'status': 'error',
            'message': '数据库配置未初始化'
        }), 500
    
    try:
        # 测试连接
        is_connected = db_config.test_connection()
        
        if is_connected:
            # 获取数据库信息
            with db_config.get_session() as session:
                from sqlalchemy import text
                result = session.execute(text("""
                    SELECT 
                        current_database() as database_name,
                        current_user as user_name,
                        version() as version
                """))
                db_info = result.fetchone()
            
            return jsonify({
                'status': 'success',
                'message': '数据库连接正常',
                'database_name': db_info.database_name,
                'user_name': db_info.user_name,
                'version': db_info.version.split(' ')[0:2]  # 只显示PostgreSQL版本
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '数据库连接失败'
            }), 500
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'数据库测试失败: {str(e)}'
        }), 500

@app.route("/fund_flow_analysis", methods=["GET", "POST"])
def fund_flow_analysis():
    """资金流分析 - 集成来源分析、聚类和可视化"""
    if request.method == "POST":
        try:
            # 处理上传的CSV文件
            uploaded_file = request.files.get('tx_file')
            if not uploaded_file or uploaded_file.filename == '':
                flash("请上传交易数据CSV文件", "warning")
                return render_template("fund_flow_analysis.html")
            
            # 读取CSV数据
            df = pd.read_csv(uploaded_file)
            
            # 验证必需字段
            required_fields = ['from_address', 'to_address', 'value']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                flash(f"CSV文件缺少必需字段: {', '.join(missing_fields)}", "danger")
                return render_template("fund_flow_analysis.html")
            
            # 获取地址标注
            known_sources_text = request.form.get("knownSources", "").strip()
            chart_style = request.form.get("chart_style", "refined")  # 获取图表样式选择
            known_sources = {}
            if known_sources_text:
                for line in known_sources_text.split('\n'):
                    if ',' in line:
                        addr, label = line.split(',', 1)
                        known_sources[addr.strip()] = label.strip()
            
            # 资金来源分析
            from modules.source_analysis import SourceAnalyzer
            analyzer = SourceAnalyzer(df)
            labeled_df = analyzer.label_sources(known_sources)
            source_stats = analyzer.aggregate_sources()
            
            # Sankey图生成 - 根据用户选择的样式
            sankey_html = 'static/temp_sankey.html'
            if chart_style == "network":
                from modules.sankey_viz import plot_network_flow
                plot_network_flow(
                    df, 
                    title="资金流向网络图",
                    output_path=sankey_html,
                    address_labels=known_sources, 
                    top_n=15
                )
            elif chart_style == "standard":
                from modules.sankey_viz import plot_sankey_standard
                plot_sankey_standard(
                    df, 
                    title="资金流向分析 (标准线条)",
                    output_path=sankey_html,
                    address_labels=known_sources, 
                    top_n=15
                )
            else:  # refined 精细样式
                from modules.sankey_viz import plot_sankey
                plot_sankey(
                    df, 
                    title="资金流向分析 (精细线条)",
                    output_path=sankey_html,
                    address_labels=known_sources, 
                    top_n=15
                )
            
            # 地址聚类分析（如果有tx_hash字段）
            cluster_results = {}
            if 'tx_hash' in df.columns:
                from modules.cluster_addresses import build_transfer_graph, cluster_addresses, analyze_clusters
                from modules.cluster_addresses import co_spend_cluster_analysis
                
                # 转账关系聚类
                transfer_graph = build_transfer_graph(df)
                if transfer_graph.number_of_nodes() > 0:
                    transfer_clusters = cluster_addresses(transfer_graph)
                    cluster_df, cluster_stats = analyze_clusters(transfer_clusters, pd.DataFrame())
                    cluster_results['transfer'] = {
                        'cluster_count': len(cluster_stats),
                        'stats': cluster_stats.to_dict('records')
                    }
                
                # Co-spend分析
                try:
                    co_spend_df, co_spend_stats = co_spend_cluster_analysis(df, pd.DataFrame())
                    if co_spend_stats is not None:
                        cluster_results['co_spend'] = {
                            'cluster_count': len(co_spend_stats),
                            'stats': co_spend_stats.to_dict('records')
                        }
                except Exception as e:
                    logger.warning(f"Co-spend分析失败: {str(e)}")
            
            # 保存结果到session（转换numpy类型为Python原生类型以支持JSON序列化）
            session['fund_flow_results'] = {
                'source_stats': source_stats.to_dict('records'),
                'cluster_results': cluster_results,
                'sankey_path': sankey_html,
                'total_transactions': int(len(df)),
                'unique_addresses': int(len(set(df['from_address']) | set(df['to_address']))),
                'total_value': float(df['value'].sum())
            }
            
            flash(f"分析完成！共处理 {len(df)} 笔交易", "success")
            
            return render_template("fund_flow_analysis.html", 
                                 source_stats=source_stats.to_dict('records'),
                                 cluster_results=cluster_results,
                                 sankey_available=True,
                                 total_transactions=len(df),
                                 unique_addresses=len(set(df['from_address']) | set(df['to_address'])),
                                 total_value=df['value'].sum())
            
        except Exception as e:
            flash(f"分析失败: {str(e)}", "danger")
            logger.error(f"资金流分析错误: {str(e)}")
            return render_template("fund_flow_analysis.html")
    
    return render_template("fund_flow_analysis.html")

@app.route("/view_sankey")
def view_sankey():
    """查看Sankey图"""
    sankey_path = session.get('fund_flow_results', {}).get('sankey_path')
    if sankey_path and os.path.exists(sankey_path):
        return send_file(sankey_path)
    else:
        flash("Sankey图不存在，请先执行分析", "warning")
        return redirect(url_for('fund_flow_analysis'))

# 🔧 应用关闭时清理资源
@app.teardown_appcontext
def cleanup_db_context(error):
    """应用上下文清理时，确保数据库连接被正确关闭"""
    try:
        if db_config:
            # 清理scoped_session
            db_config.SessionLocal.remove()
    except Exception as e:
        logger.error(f"❌ 清理数据库上下文失败: {e}")

# 🔧 定期清理连接池（可选）
@app.route('/cleanup_connections', methods=['POST'])
def cleanup_connections():
    """手动清理数据库连接池"""
    try:
        if db_config:
            cleanup_db_connections()
            return jsonify({
                'status': 'success',
                'message': '连接池清理完成'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': '数据库未初始化'
            }), 500
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'清理失败: {str(e)}'
        }), 500


# ===================== Holder数据采集管理 =====================
@app.route("/holder_collection")
def holder_collection():
    """Holder数据采集管理界面"""
    try:
        from modules.holder import list_collection_tasks
        tasks = list_collection_tasks()
        return render_template("holder_collection.html", tasks=tasks)
    except Exception as e:
        flash(f"加载采集任务失败: {e}", "danger")
        return render_template("holder_collection.html", tasks=[])

@app.route("/holder_collection/add", methods=["POST"])
def add_holder_task():
    """添加新的采集任务"""
    try:
        from modules.holder import create_collection_task
        
        task_id = request.form.get('task_id', '').strip()
        token_address = request.form.get('token_address', '').strip()
        token_symbol = request.form.get('token_symbol', '').strip()
        chain = request.form.get('chain', '').strip()
        interval_hours = int(request.form.get('interval_hours', 24))
        max_records = int(request.form.get('max_records', 1000))
        description = request.form.get('description', '').strip()
        
        if not all([task_id, token_address, token_symbol, chain]):
            flash("请填写所有必需字段", "warning")
            return redirect(url_for('holder_collection'))
        
        success = create_collection_task(
            task_id=task_id,
            token_address=token_address,
            token_symbol=token_symbol,
            chain=chain,
            interval_hours=interval_hours,
            max_records=max_records,
            description=description
        )
        
        if success:
            flash(f"成功创建采集任务: {task_id}", "success")
        else:
            flash(f"创建采集任务失败，任务可能已存在", "danger")
            
    except Exception as e:
        flash(f"创建任务失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/pause/<task_id>", methods=["POST"])
def pause_holder_task(task_id):
    """暂停采集任务"""
    try:
        from modules.holder import pause_collection_task
        success = pause_collection_task(task_id)
        if success:
            flash(f"任务 {task_id} 已暂停", "info")
        else:
            flash(f"暂停任务失败", "danger")
    except Exception as e:
        flash(f"操作失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/resume/<task_id>", methods=["POST"])
def resume_holder_task(task_id):
    """恢复采集任务"""
    try:
        from modules.holder import resume_collection_task
        success = resume_collection_task(task_id)
        if success:
            flash(f"任务 {task_id} 已恢复", "success")
        else:
            flash(f"恢复任务失败", "danger")
    except Exception as e:
        flash(f"操作失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/remove/<task_id>", methods=["POST"])
def remove_holder_task(task_id):
    """删除采集任务"""
    try:
        from modules.holder import remove_collection_task
        success = remove_collection_task(task_id)
        if success:
            flash(f"任务 {task_id} 已删除", "info")
        else:
            flash(f"删除任务失败", "danger")
    except Exception as e:
        flash(f"操作失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/run/<task_id>", methods=["POST"])
def run_holder_task_now(task_id):
    """立即执行采集任务"""
    try:
        from modules.holder import run_task_now
        
        # 在后台线程中执行，避免阻塞
        import threading
        def run_task():
            run_task_now(task_id)
        
        thread = threading.Thread(target=run_task)
        thread.daemon = True
        thread.start()
        
        flash(f"任务 {task_id} 已开始执行", "info")
    except Exception as e:
        flash(f"执行任务失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/data")
def view_all_holder_data():
    """查看所有采集数据概览"""
    try:
        from modules.holder import get_all_tasks_summary
        
        tasks_summary = get_all_tasks_summary()
        
        return render_template("holder_data.html", 
                             tasks_summary=tasks_summary)
    
    except Exception as e:
        flash(f"获取数据失败: {e}", "danger")
        return redirect(url_for('holder_collection'))

@app.route("/holder_collection/data/<task_id>")
def view_holder_data(task_id):
    """查看采集数据"""
    try:
        from modules.holder import get_task_data
        
        limit = int(request.args.get('limit', 200))
        data = get_task_data(task_id, limit)
        
        # 按时间分组数据
        snapshots_by_time = {}
        for record in data:
            snapshot_time = record['snapshot_time']
            if snapshot_time not in snapshots_by_time:
                snapshots_by_time[snapshot_time] = []
            snapshots_by_time[snapshot_time].append(record)
        
        return render_template("holder_data.html", 
                             task_id=task_id, 
                             snapshots_by_time=snapshots_by_time,
                             total_records=len(data))
    
    except Exception as e:
        flash(f"获取数据失败: {e}", "danger")
        return redirect(url_for('holder_collection'))

@app.route("/holder_collection/export/<task_id>")
def export_holder_data(task_id):
    """导出采集数据"""
    try:
        from modules.holder import export_task_data_csv
        
        csv_path = export_task_data_csv(task_id)
        if csv_path and os.path.exists(csv_path):
            return send_file(csv_path, as_attachment=True, 
                           download_name=f"holder_data_{task_id}.csv")
        else:
            flash("导出失败，请稍后重试", "danger")
            return redirect(url_for('holder_collection'))
    
    except Exception as e:
        flash(f"导出失败: {e}", "danger")
        return redirect(url_for('holder_collection'))

@app.route("/holder_collection/start_service", methods=["POST"])
def start_holder_service():
    """启动采集服务"""
    try:
        from modules.holder import start_collection_service
        start_collection_service()
        flash("采集服务已启动", "success")
    except Exception as e:
        flash(f"启动服务失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))

@app.route("/holder_collection/stop_service", methods=["POST"])
def stop_holder_service():
    """停止采集服务"""
    try:
        from modules.holder import stop_collection_service
        stop_collection_service()
        flash("采集服务已停止", "info")
    except Exception as e:
        flash(f"停止服务失败: {e}", "danger")
    
    return redirect(url_for('holder_collection'))


if __name__ == "__main__":
    # 获取端口（Render会提供PORT环境变量）
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    logger.info(f"🌐 启动Flask应用，端口: {port}")
    app.run(debug=debug, host="0.0.0.0", port=port)