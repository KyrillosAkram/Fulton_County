from bs4 import BeautifulSoup
from urllib import urlopen
import csv


def decodekeyvalue(inputdata):
	keyvalue =inputData		
	dash=keyvalue.find('-')
	while (dash !=-1) :
		keyvalue=keyvalue[0:dash]+keyvalue[1+dash:]
		dash=keyvalue.find('-')	
	space=keyvalue.find(' ')
	while (space !=-1) :
		keyvalue=keyvalue[0:space]+keyvalue[1+space:]
		space=keyvalue.find(' ')
	lL=keyvalue.find('L',2)
	if (lL!=-1): #or (ll!=-1)):
		keyvalue=keyvalue[0:lL]+"++"+keyvalue[lL:]
	ll=keyvalue.find('l',2)
	if (ll!=-1): #or (ll!=-1)):
		keyvalue=keyvalue[0:ll]+"++"+keyvalue[ll:]
	if (inputData[0]=='1'):
		keyvalue=keyvalue[0:2]+'+'+keyvalue[2:]
	return keyvalue

keyvalue =decodekeyvalue(inputData)
print(keyvalue)

def extract(key):
	input_key		=key
	key				=decodekeyvalue(key)
	url				="https://qpublic.schneidercorp.com/Application.aspx?AppID=936&LayerID=18251&PageTypeID=4&PageID=8156&Q=1271885427&KeyValue="+key
	response		=urlopen(url)
	page			=BeautifulSoup(response,'lxml')
	Parcel_Number   =page.find("td",text="Parcel Number").nextSibling()
	Location_Address=page.find("td",text="Location Address").nextSibling()
	Property_Class  =page.find("td",text="Property Class").nextSibling()
	Neighborhood    =page.find("td",text="Neighborhood").nextSibling()
	Acres           =page.find("td",text="Acres").nextSibling()
	Exceptions      =page.find("td",text="Exceptions").nextSibling()
	CurrentOwner	=page.find("span",{"id":"ctlBodyPane_ctl01_ctl01_lnkOwnerName1_lblSearch"}).get_text()
	OwnerAddress	=page.find("span",{"id":"ctlBodyPane_ctl01_ctl01_lblCityStZip"}).get_text().strip(' ')
	streetAddress	=page.find("span",{"id":"ctlBodyPane_ctl01_ctl01_lblAddress1"}).get_text()
	city			=streetAddress.pop(0)
	state			=streetAddress.pop(0)
	for textx in streetAddress:
		textx=textx+' '
	zipcode			=''.join(streetAddress)
	structure		=[]
	units			=[]
	year_built_c	=[]
	total_sq_footage=[]
	if(page.find('h1',{'id':'ctlBodyPane_ctl04_lblName'})):
		num=0
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("td",text="Structure"):
			structure.append(item.nextSibling().get_text())
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("td",text="Units"):
			units.append(item.nextSibling().get_text())
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("td",text="Year Built"):
			year_built_c.append(item.nextSibling().get_text())
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("td",text="Total Sq Footage"):
			total_sq_footage.append(item.nextSibling().get_text())
		
	if(page.find('h1',{"id":"ctlBodyPane_ctl03_lblName"})):
		stories		=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Stories").nextSibling().get_text()
		construction=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Exterior Wall").nextSibling().get_text()
		style		=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="style").nextSibling().get_text()
		year_built_r=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Year built").nextSibling().get_text()
		Res_Sq_Ft	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Res Sq Ft").nextSibling().get_text()
		basement	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="basement").nextSibling().get_text()
		Full_Half	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Full Baths/Half Bath").nextSibling().get_text().split('/')
		Full_Bath	=Full_Half[0]
		Half_Bath	=Full_Half[1]
		bedrooms	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="bedrooms").nextSibling().get_text()
		Total_Value	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).find("td",text="Total Value").nextSibling().get_text()
	
	propertyHeader	=['ParcelID','Location Address','PropertyType','Neighborhood','Acreage','Exemption Codes','CurrentOwner','OwnerAddress','city','state','zipcode','structure','Units','Year Built','Sq ft','stories','construction','style','Year built','Sq Ft','basement','Full Bath','Half Bath','bedrooms','value']
	salesHeader		=['SalesDate','SalesPrice','SalesType','Buyer','Seller']

	SaleDate		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody').findNext('td').get_text()
	SalesPrice		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody').findNext('td').findNext('td').get_text()
	salesValidity	=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[7]').get_text()
	Buyer			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[8]').get_text()
	Seller			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[9]').get_text()
	
	return