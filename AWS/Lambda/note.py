# 結合するHTMLファイルのリスト
html_files = ["fig_output1.html", "fig_output2.html", "fig_output3.html"]

# 結合後のHTMLファイル名
output_file = "combined_plots.html"

# 新しいHTMLファイルの作成とアペンド
with open(output_file, "w", encoding="utf-8") as outfile:
    # HTMLのヘッダー部分
    outfile.write("<html>\n<head>\n<title>Combined Plots</title>\n</head>\n<body>\n")
    
    # 各HTMLファイルの内容を順番に追加
    for file in html_files:
        with open(file, "r", encoding="utf-8") as infile:
            content = infile.read()
            outfile.write(content + "\n")  # 内容全体をそのままアペンド
    
    # HTMLの終了タグ
    outfile.write("</body>\n</html>")

print(f"Combined HTML file created: {output_file}")