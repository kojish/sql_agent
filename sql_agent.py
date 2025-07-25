from base_postgres import BasePostgresClient
from tabulate import tabulate
import boto3
import json
import re

# RDS PostgreSQL接続情報
DB_HOST = ""        # RDSのエンドポイント
DB_PORT = 5432
DB_NAME = ""        # データベース名
DB_USER = "postgres"        # ユーザー名
DB_PASSWORD = "" # パスワード

#PostgreSQL からスキーマ情報を取得
def get_table_info(db, schema_name, table_name):
    result = db.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema_name, table_name), fetch=True)

    if not result:
        return f"スキーマ '{schema_name}' にテーブル '{table_name}' は存在しません。"

    # 辞書型の行に対応
    full_table_name = f"{schema_name}.{table_name}"
    schema = {full_table_name: [f"{row['column_name']}: {row['data_type']}" for row in result]}
    schema_text = f"- テーブル: {full_table_name}\n? - " + "\n? - ".join(schema[full_table_name])
    return schema_text

#PostgreSQL の複数テーブルからスキーマ情報を取得
def get_multiple_table_info(db, schema_name, table_names):
    schema_texts = []
    for table_name in table_names:
        result = db.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name), fetch=True)

        if not result:
            schema_texts.append(f"スキーマ '{schema_name}' にテーブル '{table_name}' は存在しません。")
            continue

        # クエリで使うときにクォートを追加
        full_table_name = f'"{schema_name}"."{table_name}"'
        schema = [f"{row['column_name']}: {row['data_type']}" for row in result]
        schema_text = f"- テーブル: {full_table_name}\n? - " + "\n? - ".join(schema)
        schema_texts.append(schema_text)

    return "\n\n".join(schema_texts)

# Claude 3.5 Sonnet v2 でSQLを生成
def generate_sql_with_schema(user_question, schema_info):
    bedrock = boto3.client("bedrock-runtime", region_name="ap-northeast-1")

    prompt = f"""
あなたはPostgreSQLのSQLクエリを生成するアシスタントです。
以下のスキーマ情報に基づいて、ユーザーの質問に対するSQLクエリを生成してください。

**注意事項**：
- テーブル名と列名の両方にダブルクオート（"）を必ず付けてください。
  例：SELECT "列名" FROM "スキーマ名"."テーブル名";
- 数値演算（SUM, AVG, MAX, MINなど）を行う場合、対象の列がtext型であれば、integerまたはnumeric型にキャストしてください。
  例：SUM("数量"::integer)
- 複数テーブルにまたがる情報が必要な場合は、適切なJOINを使用してください。
  JOIN条件は、主キーや外部キーの関係を推定して設定してください。
- SQLは読みやすいようにインデントを付けて整形してください。
- NULL値の扱いに注意し、必要に応じてCOALESCEなどを使用してください。
- 質問に曖昧な点がある場合は、最も一般的な解釈を採用してください。
- GROUP BYやORDER BYなどの集計・並び替えが必要な場合は、適切に使用してください。
- 可能であれば、コメントを付けて各部分の意味を説明してください。
- 存在しないテーブルや列名を使用しないでください。スキーマ情報に含まれる列のみを使用してください。

--- スキーマ ---
{schema_info}
--- 質問 ---
{user_question}

--- SQLクエリ ---
"""

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 500,
        "temperature": 0.2
    }

    response = bedrock.invoke_model(
        modelId="apac.anthropic.claude-3-5-sonnet-20241022-v2:0",
        #modelId="apac.anthropic.claude-3-7-sonnet-20250219-v1:0",
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(response['body'].read())
    raw_text = result['content'][0]['text'].strip()

    sql_match = re.search(r"```sql\s*(.*?)```", raw_text, re.DOTALL | re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()
    else:
        sql_fallback = re.search(r"(SELECT|INSERT|UPDATE|DELETE).*?;", raw_text, re.IGNORECASE | re.DOTALL)
        if sql_fallback:
            return sql_fallback.group(0).strip()
        else:
            return raw_text

def main():
    db = BasePostgresClient(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    db.connect()

    # 複数テーブルのスキーマを取得
    table_list1 = ['"MARA"', '"MARC"', '"MARD"', '"MSEG"']
    table_list = ["MARA", "MARC", "MARD", "MSEG"]
    schema_name = "inc_sap_test"
    schema = get_multiple_table_info(db, schema_name, table_list)  # ← スキーマ名は適宜変更

    #print(schema)

    while True:
        user_input = input("質問を入力してください: ")
        if user_input.lower() == 'exit':
            db.close()
            print("Bye.")
            break
        
        sql = generate_sql_with_schema(user_input, schema)
        print(f"\n生成されたSQL:\n{sql}\n")

        try:
            result = db.execute(sql, fetch=True)
            if result:
                #DictRow を辞書に変換
                dict_rows = [dict(row) for row in result]
                print(tabulate(dict_rows, headers="keys", tablefmt="grid"))
            else:
                print("結果がありません。")
        except Exception as e:
            print(f"SQL実行中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()

