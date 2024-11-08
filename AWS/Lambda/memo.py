
# 結合するHTMLファイルのリスト
html_files = ["plot1.html", "plot2.html", "plot3.html", "plot4.html", "plot5.html", 
              "plot6.html", "plot7.html", "plot8.html", "plot9.html", "plot10.html",
              "plot11.html", "plot12.html", "plot13.html", "plot14.html"]  # 必ず14個すべてをリストに含める

# 結合後のHTMLファイル名
output_file = "combined_plots.html"

# 初期化: 最初のHTMLファイルからスクリプトとスタイルを抽出
scripts = ""
styles = ""
body_contents = []

for idx, file in enumerate(html_files):
    try:
        with open(file, "r", encoding="utf-8") as infile:
            content = infile.read()
            print(f"Processing file: {file}")  # デバッグ: 読み込んでいるファイル名を出力
            
            # <head>タグ内のスクリプトとスタイルを抽出（最初のファイルのみ）
            if idx == 0:
                head_start = content.find("<head>") + len("<head>")
                head_end = content.find("</head>")
                head_content = content[head_start:head_end]
                
                # スクリプトとスタイルを分離
                scripts += "\n".join([line for line in head_content.splitlines() if "script" in line or "style" in line])
                print("Extracted scripts and styles from the first file.")  # デバッグ: スクリプトとスタイル抽出の確認

            # <body>タグ内の内容を抽出
            body_start = content.find("<body>") + len("<body>")
            body_end = content.find("</body>")
            body_content = content[body_start:body_end]

            # 各プロットのIDをユニークにするために置き換え
            unique_body_content = body_content.replace("plot", f"plot_{idx}")
            body_contents.append(f"<div>{unique_body_content}</div>")
            print(f"Added content for file {file} with unique ID plot_{idx}.")  # デバッグ: 各プロットの追加を確認

    except Exception as e:
        print(f"Error processing file {file}: {e}")  # エラーメッセージ出力

# HTMLファイルの作成
try:
    with open(output_file, "w", encoding="utf-8") as outfile:
        # HTMLヘッダー部分
        outfile.write("<html>\n<head>\n<title>Combined Plots</title>\n")
        outfile.write(scripts)  # 最初のHTMLファイルのスクリプトとスタイルのみ
        outfile.write("\n</head>\n<body>\n")
        
        # 各プロットの内容を<body>内に順に追加
        for idx, body_content in enumerate(body_contents):
            outfile.write(body_content + "\n")
            print(f"Writing content for section {idx + 1}.")  # デバッグ: 各セクションが書き込まれたことを確認
        
        # HTMLの終了タグ
        outfile.write("</body>\n</html>")

    print(f"Combined HTML file created: {output_file} with {len(body_contents)} sections.")
except Exception as e:
    print(f"Error writing the combined HTML file: {e}")  # エラーメッセージ出力