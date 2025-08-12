from flask import Flask, render_template, request, session, send_file, redirect, url_for, flash
from modules import top_earners, smart_accounts, gmgn
from utils import fetch_data, export_to_excel
import time
import pandas as pd
import json

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 设置安全的密钥
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/top_earners", methods=["GET", "POST"])
def top_earners_view():
    token_address = request.args.get("tokenAddress", request.form.get("tokenAddress", ""))
    token_address = token_address.strip()
    
    if request.method == "POST" and token_address:
        try:
            traders = top_earners.fetch_top_traders(token_address)
            df = top_earners.prepare_traders_data(traders)
            
            # 保存完整数据到session
            session['traders_data'] = df.to_dict()
            session['token_address'] = token_address
            
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
            
            return render_template(
                "top_earners.html", 
                table=table_html, 
                token=token_address, 
                record_count=len(df)
            )
        except Exception as e:
            flash(f"查询失败: {str(e)}", "danger")
            return redirect(url_for('top_earners_view'))
    
    # 从session恢复数据（页面刷新时）
    if 'traders_data' in session and session.get('token_address') == token_address:
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
                record_count=len(df)
            )
    
    return render_template("top_earners.html", token=token_address)

@app.route("/download_top_earners", methods=["POST"])
def download_top_earners():
    token_address = request.form.get("tokenAddress", "").strip()
    if not token_address:
        flash("缺少代币地址", "danger")
        return redirect(url_for('top_earners_view'))
    
    try:
        # 优先使用session中的数据
        if 'traders_data' in session and session.get('token_address') == token_address:
            df = pd.DataFrame(session['traders_data'])
        else:
            traders = top_earners.fetch_top_traders(token_address)
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
        if not ca_address or not ca_name:
            flash("请输入CA地址以及名称", "danger")
            return render_template("gmgn.html")
        try:
            holders = gmgn.fetch_top_holders(chain_id, ca_address)
            traders = gmgn.fetch_top_traders(chain_id, ca_address)
            result = gmgn.merge_and_format(holders, traders, ca_name)
            result_json_str = json.dumps(result, ensure_ascii=False, indent=2)
        except Exception as e:
            flash(f"查询失败: {str(e)}", "danger")
    return render_template("gmgn.html", result_json_str=result_json_str)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)