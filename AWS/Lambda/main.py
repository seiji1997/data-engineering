# 結合するHTMLファイルのリスト
html_files = ["plot1.html", "plot2.html", "plot3.html"]  # 必要なファイル名に置き換え

# 結合後のHTMLファイル名
output_file = "combined_plots.html"

# 初期化: 最初のHTMLファイルからスクリプトとスタイルを抽出
scripts = ""
styles = ""
body_contents = []

for idx, file in enumerate(html_files):
    with open(file, "r", encoding="utf-8") as infile:
        content = infile.read()
        
        # <head>タグ内のスクリプトとスタイルを抽出（最初のファイルのみ）
        if idx == 0:
            head_start = content.find("<head>") + len("<head>")
            head_end = content.find("</head>")
            head_content = content[head_start:head_end]
            
            # スクリプトとスタイルを分離
            scripts += "\n".join([line for line in head_content.splitlines() if "script" in line or "style" in line])

        # <body>タグ内の内容を抽出
        body_start = content.find("<body>") + len("<body>")
        body_end = content.find("</body>")
        body_content = content[body_start:body_end]

        # 各プロットのIDをユニークにするために置き換え
        unique_body_content = body_content.replace("plot", f"plot_{idx}")
        body_contents.append(f"<div>{unique_body_content}</div>")

# HTMLファイルの作成
with open(output_file, "w", encoding="utf-8") as outfile:
    # HTMLヘッダー部分
    outfile.write("<html>\n<head>\n<title>Combined Plots</title>\n")
    outfile.write(scripts)  # 最初のHTMLファイルのスクリプトとスタイルのみ
    outfile.write("\n</head>\n<body>\n")
    
    # 各プロットの内容を<body>内に順に追加
    for body_content in body_contents:
        outfile.write(body_content + "\n")
    
    # HTMLの終了タグ
    outfile.write("</body>\n</html>")

print(f"Combined HTML file created: {output_file}")