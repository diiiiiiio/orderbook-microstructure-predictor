"""把 .docx 抽成纯文本（不依赖第三方库）。用法：python scripts/docx2txt.py in.docx out.txt"""
import re
import sys
import zipfile

src, dst = sys.argv[1], sys.argv[2]
xml = zipfile.ZipFile(src).read("word/document.xml").decode()
lines = []
for p in re.findall(r"<w:p[ >].*?</w:p>", xml, flags=re.S):
    t = "".join(re.findall(r"<w:t[^>]*>(.*?)</w:t>", p, flags=re.S))
    if t.strip():
        lines.append(t)
open(dst, "w").write("\n".join(lines))
print(len(lines), "paragraphs ->", dst)
