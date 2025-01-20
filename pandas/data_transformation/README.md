# 処理

|ステップ|処理|処理の詳細|得られるカラム|
|:--:|:--:|:--:|:--:|
||def 基本データ作成|||
|1|WECyc作成|ファイル名の_の前を使用する（例：3000_0.csvならDR = 3000）|WECyc|
|2|DR作成|ファイル名の_の後ろを使用する（例：3000_0.csvならDR = 0）|DR|
|3|WECycからBlockID作成|we3000とwe10000についてそれぞれにBlockID1,2を割り当てる|BlockID|
|4|uid作成|（8桁の乱数_8桁の乱数_8桁の乱数_8桁の乱数）を1種生成する|uid|
||def pageデータ作成|||
|5|seg合算|segの0,1,2,3を合算する（unitごと、shiftIndexごとにgroupbyして合算する）|seg合算値|
|6|shiftIndexを削除|不要なカラムは削除する|---|
|7|fbcX(X = A,B,C,D,E,F,G)を選択する|shiftX(X = A,B,C,D,E,F,G)のデータについて最も0に近い値（プラスでもマイナスでも0に近ければ選択する）を選んで、その時のfbcXをFBCのカラムとして生成する|FBC|
|8|shiftX(X = A,B,C,D,E,F,G)削除|不要なカラムは削除する|---|
|9|pageを作成する|Lower=D, Middle=A+C+F, Upper=B+E+G|page|
||def アドレス情報を作成|||
|10|stringを作成|stringは0,1,2,3で良いので、順番に割り当てる|string|
|11|WLを作成|Wl = unit/string　なので今回は112 = 448/4　となる|WL|
