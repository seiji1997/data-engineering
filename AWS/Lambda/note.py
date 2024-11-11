# 結合するHTMLファイルのリスト
html_files = ["plot1.html", "plot2.html", "plot3.html"]  # 必要なファイル名に置き換え

# 結合後のHTMLファイル名
output_file = "combined_plots.html"

# PlotlyのCDNスクリプト
cdn_script = """
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
"""

# CSSスタイルの設定
dark_theme_css = """
<style>
    body {
        background-color: black;
        color: white;
    }
    .plot-container {
        background-color: black;
    }
</style>
"""

body_contents = []

for idx, file in enumerate(html_files):
    with open(file, "r", encoding="utf-8") as infile:
        content = infile.read()
        
        # 各HTMLファイルには<body>タグが含まれていないため、全体をそのまま取得
        unique_body_content = content.replace("plot", f"plot_{idx}")
        body_contents.append(f"<div>{unique_body_content}</div>")

# HTMLファイルの作成
with open(output_file, "w", encoding="utf-8") as outfile:
    # HTMLヘッダー部分
    outfile.write("<html>\n<head>\n<title>Combined Plots</title>\n")
    outfile.write(cdn_script)  # CDN経由のPlotlyスクリプトを1回だけ読み込む
    outfile.write(dark_theme_css)  # 暗いテーマのCSSを追加
    outfile.write("\n</head>\n<body>\n")
    
    # 各プロットの内容を<body>内に順に追加
    for body_content in body_contents:
        outfile.write(body_content + "\n")
    
    # HTMLの終了タグ
    outfile.write("</body>\n</html>")

print(f"Combined HTML file created: {output_file}")