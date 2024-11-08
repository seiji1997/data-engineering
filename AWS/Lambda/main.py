
# 結合するHTMLファイルのリスト
html_files = ["plot1.html", "plot2.html", "plot3.html"]  # 必要なファイル名に置き換え

# 結合後のHTMLファイル名
output_file = "combined_plots.html"

with open(output_file, "w", encoding="utf-8") as outfile:
    # HTMLのヘッダーを書き込み
    outfile.write("<html><head><title>Combined Plots</title></head><body>\n")

    for file in html_files:
        with open(file, "r", encoding="utf-8") as infile:
            # bodyタグ内の内容を抽出して結合
            content = infile.read()
            body_start = content.find("<body>") + len("<body>")
            body_end = content.find("</body>")
            outfile.write(content[body_start:body_end])

    # HTMLのフッターを書き込み
    outfile.write("</body></html>")