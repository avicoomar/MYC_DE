import requests
from bs4 import BeautifulSoup

# Extract data by crawling on Shizune website i.e. https://shizune.co/investors/investors-india
def get_shizune_data():
	url = 'https://shizune.co/investors/investors-india'
	response = requests.get(url)
	html = response.text

	soup = BeautifulSoup(html, 'html.parser')

	investor_name_spans = soup.find_all("span", class_="investor__name")
	investor_names = [investor_name_span.text for investor_name_span in investor_name_spans]

	investor_meta_spans = soup.find_all("span", class_="investor__meta")
	investor_metas = [investor_meta_span.text for investor_meta_span in investor_meta_spans]

	investor_links = []
	links_divs = soup.find_all(class_="links")
	for link_div in links_divs:
		individual_links = []
		links = link_div.find_all("a")
		for link in links:
		    individual_links.append(link.get("href"))
		investor_links.append(individual_links)

	description_divs = soup.find_all("div", class_="desc")
	investor_descs = [description_div.text for description_div in description_divs]

	investment_focus_list = []
	portfolio_highlights_list_with_links = []
	portfolio_feature_list_divs = soup.find_all("div", class_="portfolio-feature-list")
	for portfolio_feature_list_div in portfolio_feature_list_divs:
		if portfolio_feature_list_div.span.text.strip() == "Investment focus":
		    investment_focuses = [list_item.text for list_item in portfolio_feature_list_div.ul.find_all("li")]
		    investment_focus_list.append(investment_focuses)
		if portfolio_feature_list_div.span.text.strip() == "Portfolio highlights":
		    portfolio_highlights = [list_item.a.get("href") + " — " + list_item.text for list_item in portfolio_feature_list_div.ul.find_all("li")]
		    portfolio_highlights_list_with_links.append(portfolio_highlights)
	
	return {
		"investor_names": investor_names,
        "investor_metas": investor_metas,
        "investor_links": investor_links,
        "investor_descs": investor_descs,
        "investment_focus_list": investment_focus_list,
        "portfolio_highlights_list_with_links": portfolio_highlights_list_with_links
	}
