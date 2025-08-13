from flask import Flask, render_template, request, session, send_file, redirect, url_for, flash
from modules import top_earners, smart_accounts, gmgn
from utils import fetch_data, export_to_excel
import time
import pandas as pd
import json
# 新增的模块
from modules import holder, parse_transactions, estimate_costs, cluster_addresses
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 设置安全的密钥
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.route("/")
def index():
    """首页"""
    return render_template("index.html")

@app.route("/top_earners", methods=["GET", "POST"])
def top_earners_view():
    chain_id = request.args.get("chainId", request.form.get("chainId", ""))
    chain_id = chain_id.strip()
    token_address = request.args.get("tokenAddress", request.form.get("tokenAddress", ""))
    token_address = token_address.strip()
    # 新增limit参数，默认100
    limit = request.args.get("limit", request.form.get("limit", "100"))
    try:
        limit = int(limit)
    except Exception:
        limit = 100

    if request.method == "POST" and token_address and chain_id:
        try:
            # 传递limit给数据获取函数
            traders = top_earners.fetch_top_traders(token_address, chain_id=chain_id, limit=limit)
            df = top_earners.prepare_traders_data(traders)
            
            # 保存完整数据到session
            session['traders_data'] = df.to_dict()
            session['token_address'] = token_address
            session['chain_id'] = chain_id
            session['limit'] = limit
            
            # 添加地址超链接
            if not df.empty and "holderWalletAddress" in df.columns:
                df["holderWalletAddress"] = df["holderWalletAddress"].apply(
                    lambda x: f'<a href="/address/{x}" title="{x}">{x[:5]}...{x[-5:]}</a>'
                )
            
            # 添加浏览器链接
            if not df.empty and "explorerUrl" in df.columns:
                df["explorerUrl"] = df["explorerUrl"].apply(
                    lambda x: f'<a href="{x}" target="_blank">查看</a>' if x else ""
                )
            
            # 格式化时间戳
            if not df.empty and "lastTradeTime" in df.columns:
                df["lastTradeTime"] = pd.to_datetime(df["lastTradeTime"], unit='ms')
            
            # 只显示部分关键字段
            display_columns = [
                "holderWalletAddress", "explorerUrl", "realizedProfit", "realizedProfitPercentage",
                "buyCount", "buyValue", "sellCount", "sellValue", "holdAmount", 
                "holdVolume", "lastTradeTime"
            ]
            
            # 过滤出存在的列
            display_df = df[[col for col in display_columns if col in df.columns]]
            
            table_html = display_df.to_html(
                classes="table table-hover table-bordered table-striped", 
                index=False, 
                escape=False,
                render_links=True
            ) if not display_df.empty else ""
            
            # 渲染模板时传递limit
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

    # session恢复时也要传递limit
    if 'traders_data' in session and session.get('token_address') == token_address and session.get('chain_id') == chain_id and session.get('limit', 100) == limit:
        df = pd.DataFrame(session['traders_data'])
        
        if not df.empty:
            # 只显示部分关键字段
            display_columns = [
                "holderWalletAddress", "explorerUrl", "realizedProfit", "realizedProfitPercentage",
                "buyCount", "buyValue", "sellCount", "sellValue", "holdAmount", 
                "holdVolume", "lastTradeTime"
            ]
            
            # 过滤出存在的列
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

@app.route("/gmgn", methods=["GET", "POST"])
def gmgn_tool():
    result_json_str = None
    if request.method == "POST":
        ca_address = request.form.get("caAddress", "").strip()
        ca_name = request.form.get("caName", "").strip()
        chain_id = request.form.get("chainId", "501").strip()
        remark_type = request.form.get("remarkType", "gmgn")
        if not ca_address or not ca_name:
            flash("请输入CA地址以及名称", "danger")
            return render_template("gmgn.html")
        try:
            holders = gmgn.fetch_top_holders(chain_id, ca_address)
            traders = gmgn.fetch_top_traders(chain_id, ca_address)
            result = gmgn.merge_and_format(holders, traders, ca_name)
            if remark_type == "gmgn":
                result_json_str = json.dumps(result, ensure_ascii=False, indent=2)
            else:  # okx备注
                okx_lines = []
                for item in result:
                    addr = item.get("address", "")
                    name = item.get("name", "")
                    if addr and name:
                        okx_lines.append(f"{addr}:{name}")
                result_json_str = ",".join(okx_lines)
        except Exception as e:
            flash(f"查询失败: {str(e)}", "danger")
    return render_template("gmgn.html", result_json_str=result_json_str)

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)