from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import datetime
import pymysql
import os
from dotenv import load_dotenv
import re
import requests
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

today_date = datetime.datetime.today()  # 오늘 날짜
target_date = today_date - datetime.timedelta(days=1)  # 오늘 날짜의 전날

# 오늘 날짜를 문자열로 변환
today_date_str = today_date.strftime("%Y-%m-%d")
target_date_str = target_date.strftime("%Y-%m-%d")

# MySQL 연결 설정
load_dotenv()  # .env 파일 읽기

def connect_to_db():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        charset='utf8'
    )

# 테이블 생성
def create_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS 기업별_뉴스횟수Final (
        id INT AUTO_INCREMENT PRIMARY KEY,
        날짜 DATE NOT NULL,
        기업명 VARCHAR(255),
        종목코드 VARCHAR(6),
        시장 VARCHAR(10),
        업종 VARCHAR(255),
        업종_ID INT,
        나온횟수 INT,
        뉴스링크 TEXT,
        뉴스제목 TEXT,
        UNIQUE (날짜, 기업명, 종목코드)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("[INFO] 테이블 생성 또는 확인 완료.")

# 상장법인목록 데이터 로드
def load_stock_data(cursor):
    query = '''
    SELECT 상장법인목록_업종세분화.code, 상장법인목록_업종세분화.name, 상장법인목록_업종세분화.market, type_name.type_name, type_name.type_ID
    FROM 상장법인목록_업종세분화
    JOIN type_name ON 상장법인목록_업종세분화.type_ID = type_name.type_ID
    '''
    cursor.execute(query)
    return {str(row[0]).strip().zfill(6): {
        'name': row[1].strip(),
        'market': row[2].strip() if row[2] else "기타",  # 시장 정보
        'type_name': row[3].strip(),  # 업종명
        'type_ID': row[4]  # 업종 ID
    } for row in cursor.fetchall()}

# 테이블 생성 후 존재 여부 확인
def ensure_table_exists(cursor):
    try:
        cursor.execute("SHOW TABLES LIKE '기업별_뉴스횟수Final'")
        result = cursor.fetchone()
        if not result:
            print("[INFO] 테이블이 존재하지 않습니다. 생성 중...")
            create_table(cursor)
            conn.commit()
        else:
            print("[INFO] 테이블이 이미 존재합니다.")
    except Exception as e:
        print(f"[ERROR] 테이블 확인 및 생성 중 오류 발생: {e}")

# 데이터베이스에 이미 저장된 뉴스 링크를 불러오기
def get_existing_news_links(cursor, date_str):
    query = "SELECT 뉴스링크 FROM 기업별_뉴스횟수Final WHERE 날짜 = %s"
    cursor.execute(query, (date_str,))
    existing_links = {row[0] for row in cursor.fetchall()}
    print(f"[INFO] {len(existing_links)}개의 기존 뉴스 링크가 데이터베이스에 존재합니다.")
    return existing_links

def fetch_news_links(url, target_date, existing_links):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)

    news_links = []
    seen_links = existing_links
    last_found_date = None

    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        news_items = soup.select("div.grid-nm.id_thum_stock_news ul.targetAdd li a")
        new_links_found = False

        for item in news_items:
            try:
                link = item.get("href")
                if not link:
                    continue
                full_link = link if link.startswith("https") else f"https://m.edaily.co.kr{link}"

                # 뉴스 날짜 및 시간 가져오기
                date_span = item.find_next("span", class_="data_info").find("span")
                if date_span:
                    date_text = date_span.text.strip()
                    news_date = datetime.datetime.strptime(date_text.split(" ")[0], "%Y-%m-%d")

                    # 기존 링크는 제외하지만 크롤링은 계속 진행
                    if full_link in seen_links:
                        print(f"[INFO] 기존 뉴스 링크 발견, 크롤링은 계속 진행: {full_link}")
                        continue

                    # 뉴스 날짜가 target_date 이후인지 확인
                    if news_date >= target_date:
                        news_links.append((full_link, date_text))
                        seen_links.add(full_link)
                        new_links_found = True
                        last_found_date = news_date
                    else:
                        print(f"[INFO] {target_date.strftime('%Y-%m-%d')} 이전 뉴스 발견. 크롤링 종료.")
                        driver.quit()
                        return news_links, last_found_date

            except Exception as e:
                print(f"[ERROR] 뉴스 링크 처리 중 오류 발생: {e}")

        if not new_links_found:
            print("[INFO] 더 이상 새로운 뉴스 링크가 발견되지 않음. 크롤링 종료.")
            break

        # 더보기 버튼 클릭
        try:
            more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "btn_page_more_con"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", more_button)
            more_button.click()
            print("[INFO] '더보기' 버튼 클릭 완료.")
            time.sleep(3)
        except Exception as e:
            print(f"[INFO] '더보기' 버튼 클릭 실패 또는 더 이상 버튼 없음: {e}")
            break

    driver.quit()
    print(f"[INFO] 뉴스 링크 크롤링 완료. 총 {len(news_links)}개 링크 발견.")
    return news_links, last_found_date



# 뉴스 데이터 저장
def process_news_data(news_links, db_stock_data, cursor, conn):
    for full_link, date_text in news_links:
        try:
            response = requests.get(full_link, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # 뉴스 제목 추출
            title = soup.title.string.strip() if soup.title else "제목 없음"

            # 뉴스에서 종목 코드 추출
            stock_codes = set(re.findall(r"\b\d{6}\b", soup.text))
            for stock_code in stock_codes:
                if stock_code not in db_stock_data:
                    continue

                company_name = db_stock_data[stock_code]["name"]
                market_type = db_stock_data[stock_code]["market"]
                company_type = db_stock_data[stock_code]["type_name"]
                type_ID = db_stock_data[stock_code]["type_ID"]

                # 데이터베이스 중복 확인
                query = """
                SELECT 뉴스링크, 뉴스제목 FROM 기업별_뉴스횟수Final 
                WHERE 날짜 = %s AND 기업명 = %s AND 종목코드 = %s
                """
                cursor.execute(query, (date_text.split(" ")[0], company_name, stock_code))
                existing_data = cursor.fetchone()

                # 기존 데이터가 존재하고 뉴스 링크 또는 제목이 포함되어 있다면 처리하지 않음
                if existing_data:
                    existing_links, existing_titles = existing_data
                    if full_link in existing_links or title in existing_titles:
                        print(f"[INFO] 기존 데이터와 중복: {company_name}, {date_text}, {full_link}")
                        continue

                # 데이터베이스에 새로 삽입
                query = """
                INSERT INTO 기업별_뉴스횟수Final 
                    (날짜, 기업명, 종목코드, 시장, 업종, 업종_ID, 나온횟수, 뉴스링크, 뉴스제목)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    뉴스링크 = CASE 
                        WHEN NOT FIND_IN_SET(%s, 뉴스링크) THEN CONCAT(뉴스링크, ' | ', %s)
                        ELSE 뉴스링크 
                    END,
                    뉴스제목 = CASE 
                        WHEN NOT FIND_IN_SET(%s, 뉴스제목) THEN CONCAT(뉴스제목, ' | ', %s)
                        ELSE 뉴스제목 
                    END,
                    `나온횟수` = LENGTH(뉴스링크) - LENGTH(REPLACE(뉴스링크, ' | ', '')) + 1
                """
                data = (date_text.split(" ")[0], company_name, stock_code, market_type, company_type, type_ID, 
                        1, full_link, title, full_link, full_link, title, title)
                cursor.execute(query, data)
                conn.commit()
                print(f"[INFO] 저장 완료: {company_name} - {date_text}")

        except Exception as e:
            print(f"[ERROR] 뉴스 크롤링 실패: {full_link} - {e}")

if __name__ == "__main__":
    conn = connect_to_db()
    cursor = conn.cursor()

    print("[INFO] 테이블 생성 확인 중...")
    ensure_table_exists(cursor)

    print("[INFO] 상장법인목록 데이터 로드 중...")
    db_stock_data = load_stock_data(cursor)

    if not db_stock_data:
        print("[WARNING] 상장법인목록 데이터가 비어 있습니다.")
        exit()

    url = "https://m.edaily.co.kr/NewsList/0701"

    # 크롤링 시작
    existing_links = get_existing_news_links(cursor, today_date_str)
    news_links, found_date = fetch_news_links(url, target_date, existing_links)

    if news_links:
        process_news_data(news_links, db_stock_data, cursor, conn)
        print(f"[INFO] {found_date} 뉴스 총 {len(news_links)}개 저장 완료.")

    '''
    # 나온횟수 업데이트
    def update_news_count(cursor, conn):
        query = "SELECT id, 뉴스링크 FROM 기업별_뉴스횟수Final"
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            row_id, links = row
            if not links:
                continue
            count = len(links.split(" | "))  # ' | ' 기준으로 링크 개수 계산
            update_query = "UPDATE 기업별_뉴스횟수Final SET 나온횟수 = %s WHERE id = %s"
            cursor.execute(update_query, (count, row_id))

        conn.commit()
        print(f"[INFO] 나온횟수 업데이트 완료. 총 {len(rows)}개 레코드 적용.")

    update_news_count(cursor, conn)
    '''
    cursor.close()
    conn.close()
    print("[INFO] 크롤링 및 데이터베이스 저장 완료.")