import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime

# 抓取上市和上櫃公司股票代碼
def get_all_stock_codes(market_type):
    """抓取所有股票代碼，market_type=2是上市公司，market_type=4是上櫃公司"""
    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={market_type}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find("table", {"class": "h4"})
    rows = table.find_all("tr")[1:]  # 跳過標題行

    stock_codes = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 0:
            stock_info = cols[0].text.strip()
            if stock_info:
                code = stock_info.split()[0]
                if code.isdigit():
                    stock_codes.append(code)

    return stock_codes

# 將數字轉換為千元並以會計符號顯示
def format_to_thousands(x):
    if pd.isna(x):
        return x
    return "{:,.2f}".format(x / 1000)

# 從Yahoo Finance抓取財務數據
def fetch_yahoo_financial_data(ticker):
    stock = yf.Ticker(ticker)
    quarterly_financials = stock.quarterly_financials
    operating_income = quarterly_financials.loc['Operating Income'].head(4)
    pretax_income = quarterly_financials.loc['Pretax Income'].head(4)
    return operating_income, pretax_income

# 抓取API的營收資料，並在2/1~3/10期間改用本機Excel數據
def fetch_stage_one_financial_data(stock_list, market_type):
    api_url = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L" if market_type == 2 else "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"

    try:
        # 以系統日期為今日
        today = datetime.date.today()

        # 用 date 而不是 datetime
        start = datetime.date(today.year, 2, 1)
        end   = datetime.date(today.year, 3, 10)
        special_period = start <= today <= end

        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.columns = df.columns.str.strip()
        df = df[df['公司代號'].isin(stock_list)]

        numeric_columns = [
            '營業收入-當月營收', '營業收入-上月營收', '營業收入-去年當月營收',
            '累計營業收入-當月累計營收', '累計營業收入-上月累計營收', '累計營業收入-去年累計營收'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                df[col] = df[col].apply(format_to_thousands)

        market_label = '上市' if market_type == 2 else '上櫃'
        df['市場類型'] = market_label

        df['累計營業收入-前期比較增減(%)'] = pd.to_numeric(df['累計營業收入-前期比較增減(%)'], errors='coerce')

        if special_period:
            print("⚠️ 當前時間在 2/1~3/10，將使用 Excel 數據替換「累計營業收入-前期比較增減(%)」")
            excel_path = r"G:\我的雲端硬碟\Horizon\python_stock\202412revenue.xlsx"
            excel_df = pd.read_excel(excel_path, usecols=[0, 2])
            excel_df.columns = ['公司代號', '累計營業收入-前期比較增減(%)']
            excel_df['公司代號'] = excel_df['公司代號'].astype(str)
            excel_df['累計營業收入-前期比較增減(%)'] = pd.to_numeric(excel_df['累計營業收入-前期比較增減(%)'], errors='coerce')
            df = df.merge(excel_df, on='公司代號', how='left', suffixes=('', '_excel'))
            df['累計營業收入-前期比較增減(%)'] = df['累計營業收入-前期比較增減(%)_excel'].combine_first(df['累計營業收入-前期比較增減(%)'])
            df.drop(columns=['累計營業收入-前期比較增減(%)_excel'], inplace=True)

        df_stage_one = df[df['累計營業收入-前期比較增減(%)'] > 0].copy()
        df_stage_one['Operating Income / Pretax Income Ratio'] = None
        return df, df_stage_one
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    return None, None

# 處理單隻股票的財務數據
def process_single_stock(index, row, market_type):
    ticker = f"{row['公司代號']}.{'TW' if market_type == 2 else 'TWO'}"
    try:
        operating_income, pretax_income = fetch_yahoo_financial_data(ticker)
        total_operating_income = operating_income.sum()
        total_pretax_income = pretax_income.sum()
        ratio = total_operating_income / total_pretax_income if total_pretax_income != 0 else 0
        row['Operating Income / Pretax Income Ratio'] = ratio

        for i in range(4):
            row[f'Operating Income Q{i+1}'] = format_to_thousands(operating_income.iloc[i])
            row[f'Pretax Income Q{i+1}'] = format_to_thousands(pretax_income.iloc[i])

    except Exception as e:
        print(f"Failed to fetch data for {ticker}: {e}")

    return index, row

# 抓取第二階段資料
def fetch_stage_two_financial_data(df_stage_one, market_type):
    """階段二：並行抓取通過階段一的股票的Operating Income及Pretax Income，並進行條件判斷"""

    # 讀取"EPS持股"sheet中的position欄位
    eps_file_path = r"G:\我的雲端硬碟\Horizon\Additional Data_LHF.xlsx"
    eps_df = pd.read_excel(eps_file_path, sheet_name='EPS持股')
    positions = eps_df['position'].astype(str).tolist()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_single_stock, index, row, market_type): index for index, row in df_stage_one.iterrows()}
        for future in as_completed(futures):
            index, updated_row = future.result()
            df_stage_one.loc[index] = updated_row

    df_stage_one['Operating Income / Pretax Income Ratio'] = df_stage_one['Operating Income / Pretax Income Ratio'].apply(lambda x: f"{x*100:.2f}%" if x is not None else "N/A")
    
    def determine_qualification(row):
        # 如果公司代號在"EPS持股"的position欄位中，則設定為'qualified'
        if str(row['公司代號']) in positions:
            print(f"公司代號 {row['公司代號']} 符合 EPS 持股的 position 標準，設定為 qualified")
            return 'qualified'
        
        # 白名單代號
        whitelist = ['2880', '2881', '2882', '2883', '2884', '2885', '2886', '2887', '2888', '2889', '2890', '2891', '2892', '5880']

        # 如果公司代號在白名單中，直接設定為 'qualified'
        if str(row['公司代號']) in whitelist:
            return 'qualified'

        # 否則根據原邏輯判斷
        try:
            value_float = float(row['Operating Income / Pretax Income Ratio'].rstrip('%'))
            if 70 <= value_float <= 130:
                return 'qualified'
            else:
                return 'not qualified'
        except ValueError:
            return 'not qualified'

    df_stage_one['qualification'] = df_stage_one.apply(determine_qualification, axis=1)

    print("階段二資料抓取與判斷完成")
    return df_stage_one

# 主程式
def main():
    # 抓取上市和上櫃公司的股票代碼
    stock_list_tw = get_all_stock_codes(2)  # 上市
    stock_list_two = get_all_stock_codes(4)  # 上櫃

    # 抓取上市公司資料
    df_tw, df_stage_one_tw = fetch_stage_one_financial_data(stock_list_tw, 2)
    df_two, df_stage_one_two = fetch_stage_one_financial_data(stock_list_two, 4)

    # 並行處理財務資料
    if df_stage_one_tw is not None:
        df_final_tw = fetch_stage_two_financial_data(df_stage_one_tw, 2)
    if df_stage_one_two is not None:
        df_final_two = fetch_stage_two_financial_data(df_stage_one_two, 4)

    # 合併上市和上櫃資料
    df_final = pd.concat([df_final_tw, df_final_two])

    # 輸出至CSV
    output_dir = r"G:\我的雲端硬碟\Horizon\python_stock"
    output_file = os.path.join(output_dir, "financial_data_stage_two.csv")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"財務數據已保存到 {output_file}")

if __name__ == "__main__":
    main()
