from bs4 import BeautifulSoup
import html

markup = "<a\n>Paragraph 1</a>\n    <a>Paragraph 2</a>"
soup = BeautifulSoup(markup, 'html.parser')
for tag in soup.find_all('a'):
    print(repr((tag.sourceline, tag.sourcepos, tag.string)))