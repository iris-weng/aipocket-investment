import psycopg2
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime


st.set_page_config(
    # page_title="恢复信息图标",
    # page_icon="ℹ️",
    # layout="centered",
    initial_sidebar_state="auto"
)


def sql_select(sql):
    #  # 建立資料庫連線
    user_DB = 'iris'
    password_DB = 'pocket777'
    host_DB = '10.188.200.16'
    port_DB = 5432

    # # 建立資料庫連線
    connection = psycopg2.connect(
                            database='tw', 
                            user=user_DB, 
                            password=password_DB, 
                            host=host_DB, 
                            port=port_DB)
    
    with connection.cursor() as cursor:

        cursor.execute(sql)

        result = cursor.fetchall()
    return result


data_list1 = sql_select(f'''
                       SELECT extract(year from fund_list_date) as list_year, etf_type, fund_manager_company, 
                                cname, code
                        FROM public.maincode_etf
                        where year = 2024
                        ;
                       ''')

df1 = pd.DataFrame(data_list1, columns=['list_year', 'etf_type', 'fund_manager_company', 'cname', 'code'])

data_list2 = sql_select(f'''
                       SELECT extract(year from est_date) as est_year, fund_type, inv_name, cname, code
                        FROM public.maincode_fund
                        where year = '2024' and fund_type <> 'None'
                        order by fund_type
                        ;
                       ''')

df2 = pd.DataFrame(data_list2, columns=['est_year', 'fund_type', 'inv_name', 'cname', 'code'])

# data_list3 = sql_select(f'''
#                        select extract(year from agent_effective_date) as agent_year, classification_name, 
#                                 agent_name, cname, code
#                             from public.maincode_fund_offshore 
#                             where year = '2024' and classification_name <> 'None' order by classification_name desc;
#                        ''')

data_list3 = sql_select(f'''
                       WITH offshore_list AS (
                            SELECT classification_name, agent_name, cname, code, est_date, agent_effective_date, 
                                currency
                            FROM public.maincode_fund_offshore 
                            WHERE year = '2024' AND classification_name <> 'None'
                            ),
                            offshore_first AS (
                                SELECT l.code, l.est_date, l.agent_effective_date, l.classification_name, 
                                    l.agent_name, l.cname,
                                    (SELECT da 
                                    FROM public.price_fund_offshore p 
                                    WHERE p.code = l.code 
                                    ORDER BY da ASC 
                                    LIMIT 1) AS first_da -- 取該標的時間最早的記錄
                                FROM offshore_list l
                            )
                            SELECT EXTRACT(YEAR FROM first_da) AS first_year, 
                                classification_name, agent_name, cname, code
                            FROM offshore_first
                            ORDER BY code;
                       ''')

df3 = pd.DataFrame(data_list3, columns=['agent_year', 'fund_type', 'agent_name', 'cname', 'code'])


# options = df["etf_type"].unique()
# print(options)
# print(data_list[0])

copy = pd.DataFrame()
c = pd.DataFrame()

st.title("ETF / 基金 / 境外基金投資試算")

# 2. 篩選條件
st.sidebar.header("篩選條件")

# 類型篩選
type = st.sidebar.selectbox(
    "選擇ETF/基金",
    options=['ETF', '基金', '境外基金'],
    # default=df["etf_type"].unique()
)

if type == 'ETF':
    fund_type = st.sidebar.selectbox(
    "選擇基金類型",
    options=df1["etf_type"].unique(),
    # default=['']
    )
    filtered_data1 = df1[df1['etf_type'] == fund_type]
    company = st.sidebar.selectbox(
        "選擇公司",
        options=filtered_data1["fund_manager_company"].unique(),
    )
    filtered_data11 = filtered_data1[(filtered_data1['etf_type'] == fund_type) & (filtered_data1['fund_manager_company'] == company)]
    target = st.sidebar.selectbox(
        "選擇標的",
        options=filtered_data11["cname"].unique(),
    )
    code = filtered_data11[filtered_data11['cname'] == target]['code'].iloc[0]
    year = filtered_data11[filtered_data11['cname'] == target]['list_year'].iloc[0]
    
    data_list4 = sql_select(f'''
                       with daily_prices_etf as ( 
                            select code, da, cl, 
                                    lag(cl) over (partition by code order by da) as pre_cl, 
                                    (cl / LAG(cl) OVER (partition by code ORDER BY da)) AS return1
                                from public.price where code in (select code from public.maincode_etf where year = 2024) order by da
                        )
                        select code, extract(year from da) as year, (exp(sum(ln(return1)))-1)*100 as year_return 
                                                from daily_prices_etf 
                                                where code = '{code}' group by code, year order by code, year;
                       ''')

    df_return = pd.DataFrame(data_list4, columns=['code', 'year', 'return'])

    data_list5 = sql_select(f'''
                       select code, extract(year from ex_dividend_date) as div_year, sum(cash_dividend_yield) as yd_ratio
                            from public.dividend where code = '{code}'
                            group by code, div_year order by code, div_year;
                       ''')
    
    df_divd = pd.DataFrame(data_list5, columns=['code', 'year', 'div_return'])


elif type == '基金':
    fund_type = st.sidebar.selectbox(
        "選擇基金類型",
        options=df2["fund_type"].unique(),
        index=6
    )
    filtered_data2 = df2[df2['fund_type'] == fund_type]
    company = st.sidebar.selectbox(
        "選擇公司",
        options=filtered_data2["inv_name"].unique(),
    )
    filtered_data22 = filtered_data2[(filtered_data2['fund_type'] == fund_type) & (filtered_data2['inv_name'] == company)]
    target = st.sidebar.selectbox(
        "選擇標的",
        options=filtered_data22["cname"].unique(),
    )
    code = filtered_data22[filtered_data22['cname'] == target]['code'].iloc[0]
    year = filtered_data22[filtered_data22['cname'] == target]['est_year'].iloc[0]

    data_list4 = sql_select(f'''
                       with daily_prices_fund as ( 
                            select code, da, nav, 
                                    lag(nav) over (partition by code order by da) as pre_nav, 
                                    (nav / LAG(nav) OVER (partition by code ORDER BY da)) AS return1
                                from public.price_fund 
                        )
                        select code, extract(year from da) as year, (exp(sum(ln(return1)))-1) * 100 as year_return 
                            from daily_prices_fund 
                            where code = '{code}'
                            group by code, year order by code, year;
                       ''')

    df_return = pd.DataFrame(data_list4, columns=['code', 'year', 'return'])

    data_list5 = sql_select(f'''
                       with year_div as (
                            with fund_dividend_ratio as (
                                select fd.code, fd.ex_dividend_date, fd.unit_distribution_amount,
                                    (select pf.da from public.price_fund pf where pf.code = fd.code and pf.da < fd.ex_dividend_date order by pf.da desc limit 1) as da,
                                    (select pf.nav from public.price_fund pf where pf.code = fd.code and pf.da < fd.ex_dividend_date order by pf.da desc limit 1) as nav
                                    from public.fund_dividend fd order by fd.code, fd.ex_dividend_date
                            ) select fdr.*, fdr.unit_distribution_amount/(fdr.unit_distribution_amount + fdr.nav) as divi_ratio
                                from fund_dividend_ratio fdr
                        ) select code, extract(year from ex_dividend_date) as year_div, sum(divi_ratio) as sum_div_ratio
                            from year_div where code = '{code}' group by code, year_div order by code, year_div;
                       ''')
    
    df_divd = pd.DataFrame(data_list5, columns=['code', 'year', 'div_return'])


elif type == '境外基金':
    fund_type = st.sidebar.selectbox(
    "選擇基金類型",
    options=df3["fund_type"].unique(),
    # default=df["etf_type"].unique()
    )
    filtered_data3 = df3[df3['fund_type'] == fund_type]
    company = st.sidebar.selectbox(
        "選擇公司",
        options=filtered_data3["agent_name"].unique(),
    )
    filtered_data33 = filtered_data3[(filtered_data3['fund_type'] == fund_type) & (filtered_data3['agent_name'] == company)]
    target = st.sidebar.selectbox(
        "選擇標的",
        options=filtered_data33["cname"].unique(),
    )
    code = filtered_data33[filtered_data33['cname'] == target]['code'].iloc[0]
    year = filtered_data33[filtered_data33['cname'] == target]['agent_year'].iloc[0]

    data_list4 = sql_select(f'''
                    select code, extract(year from da) as year_offshore, 
                            (exp(sum(ln(nav/nav_lag)))-1)*100 as offshore_return
                        From
                        (select o.da, o.code, o.nav, 
                            (select oo.da from public.price_fund_offshore oo where oo.da < o.da and oo.code = o.code order by oo.da desc limit 1) as da_lag, 
                            (select oo.nav from public.price_fund_offshore oo where oo.da < o.da and oo.code = o.code order by oo.da desc limit 1) as nav_lag 
                            from public.price_fund_offshore o where code = '{code}' order by da
                        ) as y
                        group by code, extract(year from da)
                        order by code, extract(year from da);
                       ''')

    df_return = pd.DataFrame(data_list4, columns=['code', 'year', 'return'])

    data_list5 = sql_select(f'''
                       with year_div as (
                            with fund_offshore_dividend_ratio as (
                                select fd.code, fd.ex_dividend_date, fd.unit_distribution_amount,
                                    (select pf.da from public.price_fund_offshore pf where pf.code = fd.code and pf.da < fd.ex_dividend_date order by pf.da desc limit 1) as da,
                                    (select pf.nav from public.price_fund_offshore pf where pf.code = fd.code and pf.da < fd.ex_dividend_date order by pf.da desc limit 1) as nav
                                    from public.fund_offshore_dividend fd order by fd.code, fd.ex_dividend_date
                            ) select fdr.*, fdr.unit_distribution_amount/(fdr.unit_distribution_amount + fdr.nav) as divi_ratio
                                from fund_offshore_dividend_ratio fdr
                        ) select code, extract(year from ex_dividend_date) as year_div, sum(divi_ratio) as sum_div_ratio
                            from year_div where nav <> 0 and code = '{code}' group by code, year_div order by code, year_div;
                       ''')
    
    df_divd = pd.DataFrame(data_list5, columns=['code', 'year', 'div_return'])


else:
    pass



st.sidebar.markdown(f"<p style='color:blue; font-size:16px; '><strong>   {target} </strong>成立年份為 <strong>{year}</strong> 年</p>", unsafe_allow_html=True)
# st.sidebar.write(f'{target}成立年份為{year}年')



# st.title("投資規劃模擬工具")
with st.expander("輸入試算條件參數", expanded=True):
    
    # 調整格式使輸入框更好對齊
    col1, col2, col3 = st.columns([1, 1, 1], gap='small')
    col4, col5, col6 = st.columns([1, 1, 1], gap='small')
    col7, col8, col9 = st.columns([1, 1, 1], gap='small')
    col10, col11, col12 = st.columns([1, 1, 1], gap='small')

    # with col3:
    #     if st.button('按钮 3'):
    #         st.write('你点击了按钮 3')

    # 投資起始參數
    start_year = col1.number_input("開始投資年份", min_value=int(year), max_value=datetime.now().year, value=int(year), key="start_year")
    current_age = col2.number_input("當前年齡", min_value=1, value=40, key="current_age")
    investment_years = col3.number_input("總投資年數(投入+提領)", min_value=1, value=20, key="investment_years")
    withdraw_year = col4.number_input("投資第幾年開始提領", min_value=0, value=2, key="withdraw_age")
    single_investment_amount = col5.number_input("單筆投入金額(萬元)", min_value=1, value=500, key="single_investment_amount")
    investment_amount = col6.number_input("定期定額金額 (萬元/年)", min_value=0, value=12, key="investment_amount")
    # final_age = col7.number_input("最終年齡", min_value=0, value=90, key="final_age")
    annual_return_rate = col8.slider("預期年化報酬率 (%)", min_value=0.0, max_value=20.0, value=7.0)
    withdraw = col9.number_input("提領金額(萬元)", min_value=0, value=100)
    drop_rate = col10.number_input("下跌幾%", min_value=0, value=20)
    add_amount = col11.number_input("價值投資金額(萬元)", min_value=0, value=100)

with st.expander(f"{target} 年報酬率資料"):
    st.table(df_return)

with st.expander(f'{target} 年股利率資料'):
    st.table(df_divd)


# 計算累積資產
def calculate_investment(starting_age, start_year, years, lump_sum, monthly, annual_return_data, annual_div_data,
                         default_return_rate, withdraw_year, withdraw, drop_rate, add_amount):
    data = []
    current_balance = lump_sum

    annual_return_dict = dict(zip(annual_return_data["year"], annual_return_data["return"]))
    annual_divd_return_dict = dict(zip(annual_div_data["year"], annual_div_data["div_return"]))

    for year_offset in range(years): #year in range(0, years + 1):
        current_year = start_year + year_offset
        age = starting_age + year_offset
        annual_investment = monthly
        # 假設年化複利
        return_rate = annual_return_dict.get(current_year, default_return_rate)
        divd_rate = annual_divd_return_dict.get(current_year, 0)

        if return_rate <= -drop_rate:
            current_balance += add_amount

        if year_offset == 0:
            current_balance = (current_balance) * (1 + return_rate / 100) * (1 + (divd_rate / 100))
        elif year_offset < (withdraw_year - 1):
            current_balance = (current_balance + annual_investment) * (1 + return_rate / 100) * (1 + (divd_rate / 100))
        else:
            current_balance = (current_balance + annual_investment) * (1 + return_rate / 100) * (1 + divd_rate / 100) - withdraw   


        data.append({
            "年齡": age,
            "年份": current_year,
            "下跌投資金額 (萬元)": add_amount if return_rate <= -drop_rate else 0,
            "單筆投入金額 (萬元)": lump_sum if year_offset == 0 else 0,
            "定期定額金額 (萬元)": annual_investment if year_offset > 0 else 0,
            "年報酬率": return_rate, 
            "年股利率": divd_rate,
            "提領金額": withdraw if age >= (starting_age + withdraw_year - 1) else 0,
            "累積資產 (萬元)": round(current_balance, 2)
        })
    return pd.DataFrame(data)

# 產生資料
investment_data = calculate_investment(
    current_age, start_year, investment_years, single_investment_amount, investment_amount, df_return, df_divd, 
    annual_return_rate, withdraw_year, withdraw, drop_rate, add_amount
)


st.markdown('', unsafe_allow_html=True)
st.header("投資計畫試算結果")
st.markdown('', unsafe_allow_html=True)

# 長條圖：帳戶總餘額
st.subheader("帳戶累積資產長條圖")
# 定義階段顏色
stage_colors = {
    '提領前': '#FFDDC1',
    '第一階段': '#C1FFD7',
    '第二階段': '#C1E1FF',
    '第三階段': '#FFFACD',
    '最終階段': '#D3D3D3'
}


# Q	Quantitative	數值型數據，連續的實數（例如收入、銷售額、溫度等）。
# O	Ordinal	        有序類別數據，有順序但無固定間距的類別（例如等級、排名等）。
# N	Nominal	        無序類別數據，純粹的類別或分類標籤（例如國家、性別、產品類型等）。
# T	Temporal	    時間數據，日期或時間字段（例如年份、日期時間）。
# G	GeoJSON     	地理數據，用於地理空間數據的可視化（如地圖）。

# 使用 +, |, 或 & 將多个圖表組合在一起。


bar_chart_data = pd.DataFrame({
    '年份': investment_data['年份'],
    '帳戶累積資產(萬元)': investment_data['累積資產 (萬元)'],
    '年齡': investment_data['年齡'],
    # '階段': ['提領前' if age < withdrawal_1_age else '第一階段' if age < withdrawal_2_age else '第二階段' if age < withdrawal_3_age else '第三階段' if age < withdrawal_3_age + withdrawal_3_years else '最終階段' for age in data['年齡']]
})

bar_chart = alt.Chart(bar_chart_data).mark_bar().encode(
    x=alt.X('年份:O', axis=alt.Axis(labelAngle=-30)),
    y='帳戶累積資產(萬元):Q',
    # color=alt.Color('階段:N', scale=alt.Scale(domain=list(stage_colors.keys()), range=list(stage_colors.values())), legend=alt.Legend(title='提領階段')),
    color=alt.condition(
        alt.datum.年份 < (start_year + withdraw_year - 1),
        alt.value('#FFE153'),
        alt.value('#A6FFA6')
    ),
    tooltip=['年份', '帳戶累積資產(萬元)', '年齡']
).properties(
    width=700,
    height=400
)
st.altair_chart(bar_chart, use_container_width=True)



# 顯示表格
st.subheader("帳戶累積資產試算表")
# st.table(investment_data)


# 自定義格式化函數
def format_numbers(value):
    # 如果是浮點數，顯示到小數點後兩位
    if isinstance(value, float):
        return f"{value:.2f}"
    return value

for col in investment_data.columns:
    investment_data[col] = investment_data[col].map(format_numbers)

investment_data.index = range(1, len(investment_data) + 1)

# st.write(investment_data.columns)
st.dataframe(
    investment_data.style.apply(lambda x: [
    f'background-color: {"#FFE153" if x < (current_age + withdraw_year - 1) else "#A6FFA6"}' for x in investment_data["年齡"]
], axis=0),
    height=(len(investment_data)+1) * 35, # None
    use_container_width=True

)