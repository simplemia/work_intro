#!/bin/sh
FIRST=1000
LAST=19999

for i in `seq $FIRST $LAST`; do
    URL="http://pinyin.sogou.com/dict/download_txt.php?id=$i"
    OUTPUT=`curl -v $URL 2>/dev/null`

    if [ -z `echo $OUTPUT | grep '<script'` ]; then
        FILENAME=`curl -sI $URL | iconv -f gbk -t utf-8 | grep -o -E 'filename="[^"]*' | sed -e 's/filename="//'`
        echo $OUTPUT | iconv -f gbk -t utf-8 | sed s/' '/'\n'/g > $FILENAME
    fi

    echo -n "$i of $LAST\r"
    if [ $i = $LAST ]; then
        echo "\ntask done!\n"
    fi
done