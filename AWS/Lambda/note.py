html_files = ["plot1.html", "plot2.html", ..., "plot14.html"]
output_file = "combined_plots.html"

scripts = """
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
"""

body_contents = []

for idx, file in enumerate(html_files):
    try:
        with open(file, "r", encoding="utf-8") as infile:
            content = infile.read()
            print(f"Processing file: {file}")

            # <body>タグ内の内容を抽出してユニークIDを付与
            body_start = content.find("<body>") + len("<body>")
            body_end = content.find("</body>")
            body_content = content[body_start:body_end]
            unique_body_content = body_content.replace("plot", f"plot_{idx}")
            body_contents.append(f"<div>{unique_body_content}</div>")

    except Exception as e:
        print(f"Error processing file {file}: {e}")

# 結合ファイルの作成
with open(output_file, "w", encoding="utf-8") as outfile:
    outfile.write("<html>\n<head>\n<title>Combined Plots</title>\n")
    outfile.write(scripts)
    outfile.write("\n</head>\n<body>\n")

    for idx, body_content in enumerate(body_contents):
        outfile.write(body_content + "\n")
        print(f"Writing content for section {idx + 1}.")

    outfile.write("</body>\n</html>")

print(f"Combined HTML file created: {output_file} with {len(body_contents)} sections.")