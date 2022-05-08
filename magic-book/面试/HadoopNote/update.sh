#两个目录同步并提交
scp -r /Users/xhchen/StudyPlan/blog/source/_posts/项目/ /Users/xhchen/StudyPlan/GiteeNote/HadoopNote/
git add -A
git commit -am "$(date "+%Y-%m-%d %H:%M:%S")"
git push