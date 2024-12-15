import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time  # 用來計算運行時間

# 調整API與stock代號根據「市場類型」判斷
def fetch_next_dividend_info(stock_code, market_type):
    if market_type == "上市":
        twse_url = "https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"
    elif market_type == "上櫃":
        twse_url = "https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"  # 若有不同API，請在此處更改
    
    try:
        response = requests.get(twse_url)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type')
        if 'application/json' in content_type:
            data = response.json()
            df = pd.DataFrame(data)

            df['Code'] = df['Code'].astype(str)
            stock_code = str(stock_code)  # 確保股票代號是字符串
            filtered_df = df[df['Code'] == stock_code]

            if not filtered_df.empty:
                minguo_date_str = filtered_df.iloc[0]['Date']
                next_ex_div_date = convert_to_western_date(minguo_date_str)
                next_dividend_amount = float(filtered_df.iloc[0]['CashDividend'])
                return next_ex_div_date, next_dividend_amount
    except requests.exceptions.RequestException as e:
        print(f"抓取API時發生錯誤: {e}")
    
    return '無法取得資料', '無法取得資料'

def convert_to_western_date(minguo_date_str):
    year = int(minguo_date_str[:3]) + 1911
    month = int(minguo_date_str[3:5])
    day = int(minguo_date_str[5:7])
    return datetime(year, month, day).strftime('%Y-%m-%d')

# 使用yfinance抓取財務數據時根據「市場類型」判斷
def get_financial_data(stock_code, market_type):
    if market_type == "上市":
        stock = yf.Ticker(f"{stock_code}.TW")
    elif market_type == "上櫃":
        stock = yf.Ticker(f"{stock_code}.TWO")
    
    annual_financials = stock.financials
    available_years = [col.year for col in annual_financials.columns]
    last_year = max(available_years)

    results = {}
    for i in range(4):  # 迴圈處理前四年度的數據
        year = last_year - i
        eps_column = f'{year}-12-31'
        
        try:
            eps = round(annual_financials.loc['Diluted EPS', eps_column], 2)
        except KeyError:
            eps = '無資料'

        dividends = stock.dividends.resample('YE').sum()
        dividend = dividends.loc[dividends.index.year == year]
        dividend = round(dividend.iloc[0], 2) if not dividend.empty else '無資料'

        results[f'前{i+1}年度'] = {
            'Year': year,
            'EPS': eps,
            'Dividend': dividend,
        }

    # 新增「前0年度」的配息邏輯
    this_year = datetime.now().year
    current_year_dividends = dividends.loc[dividends.index.year == this_year]
    current_year_dividend_sum = round(current_year_dividends.iloc[0], 2) if not current_year_dividends.empty else ''

    # 加入今年度配息的數值
    results['前0年度'] = {
        'Year': this_year,
        'Dividend': current_year_dividend_sum
    }

    return results

# 計算配發率
def calculate_payout_ratio(financial_data):
    payout_ratios = {}

    for i in range(1, 4):  # 從前1年度開始
        dividend_key = f'前{i}年度'
        eps_key = f'前{i+1}年度'  # EPS來自前一年的數據

        if financial_data[dividend_key]['Dividend'] != '無資料' and financial_data[eps_key]['EPS'] != '無資料':
            dividend = financial_data[dividend_key]['Dividend']
            eps = financial_data[eps_key]['EPS']

            if eps > 0:
                payout_ratio = round((dividend / eps) * 100, 2)
                payout_ratios[dividend_key] = f"{payout_ratio}%"
            else:
                payout_ratios[dividend_key] = '無法計算'
        else:
            payout_ratios[dividend_key] = '無法計算'

    return payout_ratios

# 抓取前一次和最新收盤價根據「市場類型」判斷
def get_additional_info(stock_code, market_type):
    if market_type == "上市":
        stock = yf.Ticker(f"{stock_code}.TW")
    elif market_type == "上櫃":
        stock = yf.Ticker(f"{stock_code}.TWO")

    actions = stock.actions
    ex_dividend_dates = actions.index[actions['Dividends'] > 0]
    last_ex_div_date = ex_dividend_dates[-1].strftime('%Y-%m-%d') if not ex_dividend_dates.empty else '無資料'

    last_close = '無資料'
    periods = ["1d", "5d", "1mo"]
    for period in periods:
        history_data = stock.history(period=period)
        if not history_data.empty:
            last_close = round(history_data['Close'].iloc[-1], 2)
            break

    return last_ex_div_date, last_close

# 抓取最近四個季度的EPS根據「市場類型」判斷
def get_quarterly_eps(stock_code, market_type):
    if market_type == "上市":
        stock = yf.Ticker(f"{stock_code}.TW")
    elif market_type == "上櫃":
        stock = yf.Ticker(f"{stock_code}.TWO")

    quarterly_financials = stock.quarterly_financials.T
    if 'Diluted EPS' in quarterly_financials.columns:
        diluted_eps_last_4 = [round(val, 2) for val in quarterly_financials['Diluted EPS'].dropna().iloc[:4]]
    else:
        diluted_eps_last_4 = ['無資料'] * 4

    diluted_eps_last_4.reverse()
    return diluted_eps_last_4

def process_stock_data(stock_code, market_type):
    financial_data = get_financial_data(stock_code, market_type)
    quarterly_eps = get_quarterly_eps(stock_code, market_type)
    last_ex_div_date, last_close = get_additional_info(stock_code, market_type)
    next_ex_div_date, next_dividend_amount = fetch_next_dividend_info(stock_code, market_type)

    payout_ratios = calculate_payout_ratio(financial_data)

    # 構建股票數據
    data = {'股票代碼': stock_code, '市場類型': market_type}  # 增加市場類型

    data[f'前0年度配息'] = financial_data['前0年度']['Dividend']

    for key, value in financial_data.items():
        if key != '前0年度':  # 已經加入前0年度，所以這裡略過
            data[f'{key} EPS'] = value['EPS']
            data[f'{key} 股息'] = value['Dividend']
            data[f'{key} 配發率'] = payout_ratios.get(key, '無法計算')

    for i in range(4):
        data[f'最近四個季度EPS{i+1}'] = quarterly_eps[i] if i < len(quarterly_eps) else '無資料'

    data['前一次除息日'] = last_ex_div_date
    data['下一次除息日'] = next_ex_div_date
    data['下一次除息金額'] = round(next_dividend_amount, 2) if isinstance(next_dividend_amount, (float, int)) else next_dividend_amount

    return data

# 主程式
def main():
    # 從CSV文件中讀取「公司代號」與「市場類型」欄位
    file_path = r'G:\我的雲端硬碟\Horizon\python_stock\financial_data_stage_two.csv'
    df = pd.read_csv(file_path)
    
    # 只保留「qualification」為 "qualified" 並且「公司代號」小於等於2500的列
    qualified_df = df[(df['qualification'] == 'qualified') & (df['公司代號'].astype(int) <= 2500)].copy()

    # 將整個「公司代號」欄位先轉換為字串型態
    qualified_df['公司代號'] = qualified_df['公司代號'].astype(str)

    stock_codes = qualified_df[['公司代號', '市場類型']].values.tolist()

    all_data_list = []

    # 記錄開始時間
    start_time = time.time()

    # 使用多線程加速處理
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(process_stock_data, stock_code, market_type): (stock_code, market_type) for stock_code, market_type in stock_codes}

        for future in as_completed(futures):
            stock_code, market_type = futures[future]
            try:
                data = future.result()
                all_data_list.append(data)
            except Exception as exc:
                print(f"{stock_code} 處理時發生錯誤: {exc}")

    # 記錄結束時間
    end_time = time.time()

    # 計算運行時間
    total_time = end_time - start_time
    print(f"程式運行總時間: {total_time:.2f} 秒")

    # 將所有股票數據轉換為 DataFrame
    all_data_df = pd.DataFrame(all_data_list)

    # 保存為 XLSX 文件到指定路徑
    output_path = r'G:\我的雲端硬碟\Horizon\python_stock\qualified_stocks_financial_data_A.xlsx'
    all_data_df.to_excel(output_path, index=False)

    print("所有股票數據已保存到 'qualified_stocks_financial_data_A.xlsx'")

if __name__ == "__main__":
    main()
