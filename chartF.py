# db데이터 가져와서 구현
import streamlit as st
import pandas as pd
import pymysql
from datetime import datetime
import time
import yfinance as yf
import subprocess
import os
from dotenv import load_dotenv 

# 페이지 설정
st.set_page_config(page_title="뉴스 집계", page_icon="📈")

# .env 파일 읽기
load_dotenv()

# 스타일 추가 (CSS)
st.markdown(
    """
    <style>
    .stButton > button {
        width: 120px;
        height: 40px;
        font-size: 16px;
        text-align: center;
        padding: 5px 0;
        background-color: #007bff;
        color: white;
        border: 1px solid #007bff;
        border-radius: 5px;
    }
    .stButton > button:hover {
        background-color: #333333;
        color: #ffffff;
        border: 1px solid black;
    }
    table {
        table-layout: auto;
        width: 100%;
        overflow-x: auto;
        white-space: nowrap;
    }
    th, td {
        text-align: left;
        padding: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# 오늘 날짜
today = datetime.now().strftime("%Y년 %m월 %d일")

# SQL 파일 실행 함수
def execute_sql_file():
    try:
        # SQL 파일 경로 지정
        sql_file_path = './sql.py'
        
        # Python 3 명시적으로 호출 (비동기 실행)
        process = subprocess.Popen(
            ['python', sql_file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # 실시간 출력 캡처
        stdout_lines = []
        stderr_lines = []
        for line in iter(process.stdout.readline, ''):
            stdout_lines.append(line.strip())
            print(f"[STDOUT] {line.strip()}")  # 터미널 출력
            st.write(f"[STDOUT] {line.strip()}")  # Streamlit 출력
        
        # 오류 메시지 캡처
        for line in iter(process.stderr.readline, ''):
            stderr_lines.append(line.strip())
            print(f"[STDERR] {line.strip()}")  # 터미널 출력
            st.error(f"[STDERR] {line.strip()}")  # Streamlit 출력
        
        process.stdout.close()
        process.stderr.close()
        process.wait()
        
        # 반환값 확인
        if process.returncode != 0:
            return None, '\n'.join(stderr_lines)
        return '\n'.join(stdout_lines), None
    except subprocess.TimeoutExpired:
        print("[ERROR] sql.py 실행이 너무 오래 걸립니다.")  # 터미널 출력
        st.error("sql.py 실행이 너무 오래 걸립니다. 확인해주세요.")  # Streamlit 출력
        return None, "sql.py 실행이 너무 오래 걸립니다."
    except Exception as e:
        print(f"[ERROR] {str(e)}")  # 터미널 출력
        st.error(str(e))  # Streamlit 출력
        return None, str(e)


# 날짜 선택 달력 추가
st.sidebar.subheader("날짜 필터")
selected_date = st.sidebar.date_input("조회할 날짜를 선택하세요", value=datetime.now().date())


# 데이터 로드 함수
@st.cache_data
def load_data_from_db():
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            charset='utf8'
        )
        query = """
        SELECT 
            날짜, 기업명, 종목코드, 시장, 업종, 나온횟수, 뉴스링크, 뉴스제목 
        FROM 
            기업별_뉴스횟수Final
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"데이터베이스 연결 및 로드 중 오류 발생: {e}")
        return pd.DataFrame()  # 빈 데이터프레임 반환
    
@st.cache_data
def get_news_counts_by_date(stock_name):
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            charset='utf8'
        )
        query = f"""
        SELECT 날짜, 나온횟수 
        FROM 기업별_뉴스횟수Final 
        WHERE 기업명 = '{stock_name}'
        ORDER BY 날짜
        """
        news_data = pd.read_sql(query, conn)
        conn.close()

        # 날짜를 datetime으로 강제 변환
        news_data['날짜'] = pd.to_datetime(news_data['날짜'], errors='coerce')

        # 변환되지 않은 값(NaT) 제거
        if news_data['날짜'].isna().any():
            st.warning("유효하지 않은 날짜가 발견되어 제거됩니다.")
            news_data = news_data.dropna(subset=['날짜'])

        return news_data
    except Exception as e:
        st.error(f"뉴스 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()


# 캔들 차트와 뉴스 차트를 결합한 HTML 생성 함수
def create_combined_chart_html(stock_data, news_counts):
    # 데이터 유효성 검사 및 변환
    stock_data = stock_data.copy()
    if 'time' not in stock_data.columns:
        stock_data['time'] = pd.to_datetime(stock_data.index, errors='coerce')
    else:
        stock_data['time'] = pd.to_datetime(stock_data['time'], errors='coerce')
    stock_data = stock_data.dropna(subset=['time'])
    stock_data['time'] = stock_data['time'].dt.strftime('%Y-%m-%d')

    news_counts = news_counts.copy()
    if '날짜' not in news_counts.columns:
        raise ValueError("뉴스 데이터에 '날짜' 컬럼이 없습니다.")
    news_counts['날짜'] = pd.to_datetime(news_counts['날짜'], errors='coerce')
    news_counts = news_counts.dropna(subset=['날짜'])
    news_counts['날짜'] = news_counts['날짜'].dt.strftime('%Y-%m-%d')

    # 나온 횟수 데이터를 스케일링
    scaled_news_counts = news_counts.copy()
    scaled_news_counts['나온횟수_스케일'] = scaled_news_counts['나온횟수'] 

    # 거래량 스케일 조정
    scaled_volume_data = stock_data.copy()
    scaled_volume_data["Volume"] = scaled_volume_data["Volume"] / 1000

    # 데이터 문자열을 HTML보다 먼저 생성 
    candle_data_str = ",".join([
        f"{{ time: '{row['time']}', open: {row['open']}, high: {row['high']}, low: {row['low']}, close: {row['close']} }}"
        for _, row in stock_data.iterrows()
    ])
    
    volume_data_str = ",".join([
        f"{{ time: '{row['time']}', value: {row['Volume']} }}"
        for _, row in scaled_volume_data.iterrows()
    ])
    
    news_data_str = ",".join([
        f"{{ time: '{row['날짜']}', value: {row['나온횟수_스케일']} }}"
        for _, row in scaled_news_counts.iterrows()
    ])


    # HTML 및 JavaScript 생성
    chart_html = f"""
    <div id="chart-container" style="height: 500px; position: relative;"></div>
    <div id="tooltip" style="
        position: absolute;
        display: none;
        background-color: rgba(255, 255, 255, 0.9);
        border: 1px solid rgba(0, 0, 0, 0.5);
        border-radius: 4px;
        padding: 8px;
        font-size: 12px;
        color: #000;
        pointer-events: none;
        z-index: 1000;
    "></div>
    <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
    <script type="text/javascript">
    document.addEventListener("DOMContentLoaded", function () {{
        if (!window.LightweightCharts) {{
            console.error("LightweightCharts 라이브러리가 로드되지 않았습니다.");
            return;
        }}

        const chartContainer = document.getElementById('chart-container');
        if (!chartContainer) {{
            console.error("차트 컨테이너를 찾을 수 없습니다.");
            return;
        }}
        const chart = LightweightCharts.createChart(chartContainer, {{
            width: 600,
            height: 500,
            layout: {{
                backgroundColor: '#ffffff',
                textColor: '#000000',
            }},
            grid: {{
                vertLines: {{ color: '#e0e0e0' }},
                horzLines: {{ color: '#e0e0e0' }},
            }},
        }});

        const candleSeries = chart.addCandlestickSeries();
        candleSeries.setData([{candle_data_str}]);

        const volumeSeries = chart.addHistogramSeries({{
            priceScaleId: 'volume',
            color: 'rgba(79, 16, 188, 0.8)',
            priceFormat: {{
                type: 'custom',
                formatter: value => value.toLocaleString() + "K",
            }},
        }});
        volumeSeries.setData([{volume_data_str}]);

        const newsSeries = chart.addLineSeries({{
            priceScaleId: 'news',
            color: 'rgba(0, 102, 255, 1.0)',
            lineWidth: 2,
            priceFormat: {{
                type: 'custom',
                formatter: value => value.toLocaleString() + "회",
            }},
        }});
        newsSeries.setData([{news_data_str}]);

        chart.priceScale('volume').applyOptions({{ scaleMargins: {{ top: 0.6, bottom: 0 }} }});
        chart.priceScale('news').applyOptions({{ scaleMargins: {{ top: 0.5, bottom: 0.2 }} }});

        const tooltip = document.getElementById('tooltip');
        chart.subscribeCrosshairMove((param) => {{
            if (!param.point || !param.time) {{
                tooltip.style.display = 'none';
                return;
            }}
            const date = param.time;
            const candleData = param.seriesData.get(candleSeries);
            const volumeData = param.seriesData.get(volumeSeries);
            const newsData = param.seriesData.get(newsSeries);

            if (candleData) {{
                tooltip.style.display = 'block';
                tooltip.innerHTML = `
                    <strong>${{date}}</strong><br>
                    Open: ${{candleData.open}}<br>
                    High: ${{candleData.high}}<br>
                    Low: ${{candleData.low}}<br>
                    Close: ${{candleData.close}}<br>
                    Volume: ${{volumeData ? volumeData.value.toLocaleString() + 'K' : 'N/A'}}<br>
                    나온 횟수: ${{newsData ? newsData.value.toLocaleString() + '회' : 'N/A'}}
                `;
                const chartRect = chartContainer.getBoundingClientRect();
                tooltip.style.left = (param.point.x + chartRect.left + 15) + 'px';
                tooltip.style.top = (param.point.y + chartRect.top + 15) + 'px';
            }} else {{
                tooltip.style.display = 'none';
            }}
        }});

        chart.timeScale().fitContent();
    }});
    </script>
    """

    return chart_html

# Progress bar
latest_iteration = st.empty()
bar = st.progress(0)

for i in range(100):
    latest_iteration.text(f'진행률 {i+1}%')
    bar.progress(i + 1)
    time.sleep(0.02)

st.balloons()

# 선택한 날짜로 데이터 필터링
def filter_data(df, selected_date):
    df['날짜'] = pd.to_datetime(df['날짜']).dt.date  # 날짜 컬럼을 datetime.date 형식으로 변환
    filtered_df = df[df['날짜'] == selected_date]  # 선택된 날짜의 데이터 필터링

    if filtered_df.empty:  # 선택한 날짜의 데이터가 없으면
        most_recent_date = df['날짜'].max()  # 가장 최근 날짜의 데이터를 가져옴
        filtered_df = df[df['날짜'] == most_recent_date]
        st.warning(f"{selected_date}에 해당하는 데이터가 없어 {most_recent_date}의 데이터를 표시합니다.")
        return filtered_df, most_recent_date  # 최근 날짜와 데이터 반환
    return filtered_df, selected_date  # 선택한 날짜와 데이터 반환


# 데이터 로드
df = load_data_from_db()

# 데이터 확인
if df.empty:
    st.error("데이터를 불러오지 못했습니다. 데이터베이스를 확인하세요.")
    st.stop()

# 선택한 날짜의 데이터 필터링
filtered_data, display_data = filter_data(df, selected_date)
   

# 상태 관리용 세션 상태 초기화
if "selected_filter" not in st.session_state:
    st.session_state.selected_filter = None
if "selected_item" not in st.session_state:
    st.session_state.selected_item = None

# 페이지 전환 로직
if st.session_state.selected_filter is None:
    # 메인 페이지
    st.write(f"""
    # {display_data} 증권 뉴스
    """)

    # 버튼을 항상 진행률 바 아래 표시
    st.write("") 

    # 새로고침 버튼만 표시
    if st.button("새로 고침", key="refresh_main"):
        with st.spinner("sql.py 실행 중입니다. 잠시만 기다려 주세요..."):
            output, error = execute_sql_file()
            
            if error:
                st.error(f"SQL 실행 중 오류 발생: {error}")
            else:
                st.success("데이터베이스가 성공적으로 업데이트되었습니다.")
                st.write(output)  # sql.py의 최종 로그를 출력
                
                st.experimental_rerun()
                # 데이터 로드 및 페이지 새로고침
                #df = load_data_from_db()
                #if df.empty:
                #    st.error("데이터를 불러오지 못했습니다. 데이터베이스를 확인하세요.")
                #else:
                #    st.experimental_rerun()



    # 선택 박스: 업종별 또는 기업별
    filter_option = st.selectbox(
        "조회할 항목을 선택하세요",
        ["기업별 TOP 10", "업종별 TOP 10"]
    )

    if filter_option == "기업별 TOP 10":
            st.subheader("기업별 집계")
            기업별_집계 = filtered_data.groupby('기업명')['나온횟수'].sum().reset_index().sort_values(by='나온횟수', ascending=False)
            기업별_집계 = 기업별_집계.reset_index(drop=True)
            기업별_집계.index = 기업별_집계.index + 1  

            #st.write("기업별 데이터 (상세보기 버튼 클릭 시 상세 페이지로 이동)")
            for idx, row in 기업별_집계[:10].iterrows():
                cols = st.columns([1, 3, 2, 1])
                with cols[0]:
                    st.write(f"**{idx}**")
                with cols[1]:
                    st.write(row['기업명'])
                with cols[2]:
                    st.write(f"{row['나온횟수']}회")  
                with cols[3]:
                    if st.button("상세보기", key=f"기업_{row['기업명']}"):
                        st.session_state.selected_filter = "기업별"
                        st.session_state.selected_item = row['기업명']
                        st.rerun()
    # 데이터 처리 및 출력
    elif filter_option == "업종별 TOP 10":
        st.subheader("업종별 집계")
        업종별_집계 = filtered_data.groupby('업종')['나온횟수'].sum().reset_index().sort_values(by='나온횟수', ascending=False)
        업종별_집계 = 업종별_집계.reset_index(drop=True)
        업종별_집계.index = 업종별_집계.index + 1  

        #st.write("업종별 데이터 (상세보기 버튼 클릭 시 상세 페이지로 이동)")
        for idx, row in 업종별_집계[:10].iterrows():
            cols = st.columns([1, 3, 2, 1])
            with cols[0]:
                st.write(f"**{idx}**")
            with cols[1]:
                st.write(row['업종'])
            with cols[2]:
                st.write(f"{row['나온횟수']}회")  
            with cols[3]:
                if st.button("상세보기", key=f"업종_{row['업종']}"):
                    st.session_state.selected_filter = "업종별"
                    st.session_state.selected_item = row['업종']
                    st.rerun()

    

else:
    # 상세 페이지

    if st.session_state.selected_filter == "기업별":
        filtered_df = filtered_data[filtered_data['기업명'] == st.session_state.selected_item][['날짜','기업명', '종목코드', '시장','뉴스링크', '뉴스제목']]
    elif st.session_state.selected_filter == "업종별":
        filtered_df = (filtered_data[filtered_data['업종'] == st.session_state.selected_item]
            .sort_values(by='나온횟수', ascending=False))[['날짜', '기업명', '종목코드', '시장', '뉴스링크', '뉴스제목']]
        
    # 버튼을 항상 진행률 바 아래 표시
    st.write("")  # 공백 추가
    #col1, col2 = st.columns([0.05, 0.2])
    #with col1:
    if st.button("뒤로 가기", key="back_main"):
        st.session_state.selected_filter = None
        st.session_state.selected_item = None
        st.rerun()

    #with col2:
        # 새로고침 버튼만 표시
    #    if st.button("새로 고침", key="refresh_main"):
    #        with st.spinner("sql.py 실행 중입니다. 잠시만 기다려 주세요..."):
    #            output, error = execute_sql_file()
                
    #            if error:
    #                st.error(f"SQL 실행 중 오류 발생: {error}")
    #            else:
    #                st.success("데이터베이스가 성공적으로 업데이트되었습니다.")
    #                st.write(output)  # sql.py의 최종 로그를 출력
                    
    #                st.experimental_rerun()
                    # 데이터 로드 및 페이지 새로고침
                    #df = load_data_from_db()
                    #if df.empty:
                    #    st.error("데이터를 불러오지 못했습니다. 데이터베이스를 확인하세요.")
                    #else:
                    #    st.experimental_rerun()

    filtered_df = filtered_df.drop_duplicates().reset_index(drop=True)
    market_type = filtered_df['시장'].iloc[0]
    filtered_df = filtered_df.drop(columns=['시장'])
    filtered_df.index += 1  

    # 뉴스 링크에 제목을 표시
    def create_numbered_links(row):
        links = row['뉴스링크'].split('|')  
        titles = row['뉴스제목'].split('|')  
        if len(links) != len(titles):
            return "데이터 오류: 링크와 제목의 개수가 일치하지 않습니다."
        numbered_links = [f"{i + 1}. <a href='{link.strip()}' target='_blank'>{title.strip()}</a>"
                          for i, (link, title) in enumerate(zip(links, titles))]
        return '<br>'.join(numbered_links)

    # "뉴스 링크" 컬럼에 제목을 하이퍼링크로 표시
    filtered_df['뉴스링크'] = filtered_df.apply(create_numbered_links, axis=1)
    filtered_df = filtered_df.drop(columns=['뉴스제목'])  

    # 데이터 출력 (HTML 테이블 스타일 적용)
    st.write(filtered_df.to_html(escape=False), unsafe_allow_html=True)
    
    if st.session_state.selected_filter == "기업별":
        @st.cache_data
        def get_stock_data(market_type):
            return yf.download(market_type, start="2003-01-01")

        try:
            # 종목코드와 시장 정보 결합
            company_code = filtered_df['종목코드'].iloc[0]
            stock_name = st.session_state.selected_item  # 선택된 기업명
            news_data = get_news_counts_by_date(stock_name)  # 뉴스 데이터 가져오기
            
            if market_type == '코스피':
                formatted_code = f"{str(company_code).zfill(6)}.KS"
            elif market_type == '코스닥':
                formatted_code = f"{str(company_code).zfill(6)}.KQ"
            elif market_type == '코넥스':
                st.warning("코넥스는 지원 안 됩니다.")
            else:
                st.warning("시장 정보가 없거나 올바르지 않습니다.")
                formatted_code = str(company_code).zfill(6)  
                st.stop()

            # 주가 차트 출력
            st.write("")  # 공백 추가
            st.subheader(f"{st.session_state.selected_item} 주가 차트")
            st.write(f"조회 종목 코드: {formatted_code}")

            # 주가 데이터 가져오기
            stock_data = get_stock_data(formatted_code)
            print('2')
            

            # MultiIndex (튜플 컬럼) 평탄화
            if isinstance(stock_data.columns, pd.MultiIndex):
                stock_data.columns = ['_'.join([str(c) for c in col if c]) for col in stock_data.columns]

            # 인덱스가 DatetimeIndex면 일반 컬럼으로 변환
            if isinstance(stock_data.index, pd.DatetimeIndex):
                stock_data = stock_data.reset_index()

            # 모든 컬럼명을 소문자로 통일 (strip()으로 공백 제거)
            stock_data.columns = [col.strip().lower() for col in stock_data.columns]

            # 'time' 컬럼 생성 (없을 경우 대체)
            if 'time' not in stock_data.columns:
                if 'date' in stock_data.columns:
                    stock_data.rename(columns={'date': 'time'}, inplace=True)
                elif 'index' in stock_data.columns:
                    stock_data.rename(columns={'index': 'time'}, inplace=True)
                else:
                    stock_data['time'] = pd.to_datetime(stock_data.index, errors='coerce')
            
            # 컬럼명 정리: 뒤에 붙은 종목코드 제거
            cleaned_cols = []
            for col in stock_data.columns:
                if isinstance(col, tuple):
                    col = '_'.join([str(c) for c in col if c])
                base = col.split('_')[0].lower()
                cleaned_cols.append(base)
            stock_data.columns = cleaned_cols

            # 최종 검사
            if 'time' not in stock_data.columns:
                st.error("'time' 컬럼을 찾을 수 없습니다. stock_data의 컬럼을 확인하세요.")
                st.write("현재 컬럼 목록:", stock_data.columns.tolist())
            else:
                stock_data['time'] = pd.to_datetime(stock_data['time'], errors='coerce')

            # Volume 컬럼 이름이 소문자로 바뀐 경우 대비
            if 'volume' in stock_data.columns and 'Volume' not in stock_data.columns:
                stock_data.rename(columns={'volume': 'Volume'}, inplace=True)
            elif 'Volume' not in stock_data.columns:
                st.error("'Volume' 컬럼을 찾을 수 없습니다. stock_data의 컬럼을 확인하세요.")
                st.write("현재 컬럼 목록:", stock_data.columns.tolist())

            if not stock_data.empty and not news_data.empty:
                try:
                    # 날짜별 나온 횟수 정리
                    news_data['날짜'] = pd.to_datetime(news_data['날짜'], errors='coerce')  
                    news_counts = news_data.groupby('날짜', as_index=False)['나온횟수'].sum()
                    news_counts['날짜'] = news_counts['날짜'].dt.strftime('%Y-%m-%d')  

                    # 차트 생성
                    chart_html = create_combined_chart_html(stock_data, news_counts)
                    st.components.v1.html(chart_html, height=500)

                except Exception as e:
                    st.error(f"차트 생성 중 오류 발생: {e}")
            else:
                st.warning("주가 데이터 또는 뉴스 데이터가 부족합니다.")

            
            #st.write("뉴스 데이터의 컬럼 정보 및 샘플 데이터:")
            #st.write(news_data.dtypes)  # 각 컬럼의 데이터 유형
            #st.write(news_data.head())  # 데이터 샘플 확인
            #st.write("뉴스 횟수 데이터 확인:")
            #st.write(news_counts.head())


        except IndexError:
            st.error("종목코드를 가져올 수 없습니다.")
        except Exception as e:
            st.error(f"오류 발생: {e}")

