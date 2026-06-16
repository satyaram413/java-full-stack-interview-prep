#!/usr/bin/env python3
"""Generate RESUME_INTERVIEW_QA.pdf from markdown. Requires: pip install markdown xhtml2pdf"""
import re
from pathlib import Path

try:
    import markdown
    from xhtml2pdf import pisa
except ImportError:
    print("Install dependencies: pip install markdown xhtml2pdf")
    raise

ROOT = Path(__file__).parent
MD_FILE = ROOT / "RESUME_INTERVIEW_QA.md"
PDF_FILE = ROOT / "RESUME_INTERVIEW_QA.pdf"
HTML_FILE = ROOT / "RESUME_INTERVIEW_QA.html"

CSS = """
@page { size: A4; margin: 1.5cm; }
body { font-family: Helvetica, Arial, sans-serif; font-size: 10pt; line-height: 1.45; color: #222; }
h1 { font-size: 18pt; border-bottom: 2px solid #333; padding-bottom: 6px; margin-top: 0; }
h2 { font-size: 13pt; color: #1a1a1a; margin-top: 18px; page-break-after: avoid; }
h3 { font-size: 11pt; color: #333; margin-top: 14px; page-break-after: avoid; }
p { margin: 6px 0; }
ul, ol { margin: 6px 0 10px 18px; }
li { margin-bottom: 3px; }
code { font-family: Courier, monospace; font-size: 9pt; background: #f4f4f4; padding: 1px 4px; }
pre { background: #f4f4f4; padding: 8px; font-size: 8.5pt; overflow-wrap: break-word; white-space: pre-wrap; }
table { border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 9pt; }
th, td { border: 1px solid #ccc; padding: 5px 8px; text-align: left; vertical-align: top; }
th { background: #eee; }
hr { border: none; border-top: 1px solid #ddd; margin: 16px 0; }
blockquote { border-left: 3px solid #999; margin: 8px 0; padding-left: 10px; color: #444; }
strong { color: #111; }
a { color: #0645ad; text-decoration: none; }
"""

def md_to_html(md_text: str) -> str:
    body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
    )
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Resume Interview Q&A</title>
<style>{CSS}</style></head><body>{body}</body></html>"""


def html_to_pdf(html: str, pdf_path: Path) -> None:
    with open(pdf_path, "wb") as out:
        status = pisa.CreatePDF(html.encode("utf-8"), dest=out, encoding="utf-8")
    if status.err:
        raise RuntimeError(f"PDF generation failed with {status.err} errors")


def main():
    md = MD_FILE.read_text(encoding="utf-8")
    html = md_to_html(md)
    HTML_FILE.write_text(html, encoding="utf-8")
    html_to_pdf(html, PDF_FILE)
    print(f"Created: {PDF_FILE}")
    print(f"Created: {HTML_FILE}")


if __name__ == "__main__":
    main()
