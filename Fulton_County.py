from time import time
from bs4 import BeautifulSoup
from requests import get
import csv
from multiprocessing import cpu_count,Pool
import re
startTime=time()
inputs_path		="inputs.txt"
propertyHeader	=['Input','ParcelID','Location Address','PropertyType','Neighborhood','Acreage','Exemption Codes','CurrentOwner','OwnerAddress','city','state','zipcode','structure','Units','Year Built','Sq ft','stories','construction','style','Year built','Sq Ft','basement','Full Bath','Half Bath','bedrooms','value']
salesHeader		=['SalesDate','SalesPrice','SalesType','Buyer','Seller']
file2			=open(inputs_path,'r')
inputs			=[line.strip() for line in file2]
file2.close()
number_of_inputs=len(inputs)
number_of_cpus	=cpu_count()
print("found %i cpu and will be used to accelerate the proccess"%number_of_cpus)


def decodekeyvalue(inputdata):
	keyvalue =inputdata		
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
	if (inputdata[0]=='1'):
		keyvalue=keyvalue[0:2]+'+'+keyvalue[2:]
	return keyvalue



def scrape(key):
	input_key		=key
	key				=decodekeyvalue(key)
	url				="https://qpublic.schneidercorp.com/Application.aspx?AppID=936&LayerID=18251&PageTypeID=4&PageID=8156&Q=1271885427&KeyValue="+key
	print("scraping %s"%(input_key))
	response		=get(url).text
	page			=BeautifulSoup(response,'lxml')
	Parcel_Number   =page.find("strong",text='Parcel Number').findParent('td').nextSibling.nextSibling.text
	Location_Address=page.find("strong",text="Location Address").findParent('td').nextSibling.nextSibling.text
	Property_Class  =page.find("strong",text="Property Class").findParent('td').nextSibling.nextSibling.text
	Neighborhood    =page.find("strong",text="Neighborhood").findParent('td').nextSibling.nextSibling.text
	Acres           =page.find("strong",text="Acres").findParent('td').nextSibling.nextSibling.text
	Exceptions      =page.find("strong",text="Exemptions").findParent('td').nextSibling.nextSibling.text
	CurrentOwner	=page.select_one("#ctlBodyPane_ctl01_ctl01_lnkOwnerName1_lnkSearch").text
	OwnerAddress	=page.find("span",{"id":"ctlBodyPane_ctl01_ctl01_lblCityStZip"}).text.split(' ')
	streetAddress	=page.find("span",{"id":"ctlBodyPane_ctl01_ctl01_lblAddress1"}).text
	#print(OwnerAddress)
	city			=OwnerAddress.pop(0)
	state			=OwnerAddress.pop(0)
	for textx in OwnerAddress:
		textx=textx+' '
	zipcode			=''.join(OwnerAddress)
	structure		=[]
	units			=[]
	year_built_c	=[]
	total_sq_footage=[]
	if(page.find('h1',{'id':'ctlBodyPane_ctl04_lblName'})):
		num=0
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("strong",text="Structure"):
			structure.append(item.findParent('td').nextSibling.nextSibling.text)
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("strong",text="Units"):
			units.append(item.findParent('td').nextSibling.nextSibling.text)
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("strong",text="Year Built"):
			year_built_c.append(item.findParent('td').nextSibling.nextSibling.text)
		for item in page.find('section',{"id":"ctlBodyPane_ctl04_mSection"}).findAll("strong",text="Total Sq Footage"):
			total_sq_footage.append(item.findParent('td').nextSibling.nextSibling.text)
		
	if(page.find('h1',{"id":"ctlBodyPane_ctl03_lblName"})):
		stories		=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Stories").findParent('td').nextSibling.nextSibling.text
		construction=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Exterior Wall").findParent('td').nextSibling.nextSibling.text
		style		=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="style").findParent('td').nextSibling.nextSibling.text
		year_built_r=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Year built").findParent('td').nextSibling.nextSibling.text
		Res_Sq_Ft	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Res Sq Ft").findParent('td').nextSibling.nextSibling.text
		basement	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="basement").findParent('td').nextSibling.nextSibling.text
		Full_Half	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Full Baths/Half Bath").findParent('td').nextSibling.nextSibling.text.split('/')
		Full_Bath	=Full_Half[0]
		Half_Bath	=Full_Half[1]
		bedrooms	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="bedrooms").findParent('td').nextSibling.nextSibling.text
		Total_Value	=page.find('section',{"id":"ctlBodyPane_ctl03_mSection"}).findNext("strong",text="Total Value").findParent('td').nextSibling.nextSibling.text
	else:
		stories		=''
		construction=''
		style		=''
		year_built_r=''
		Res_Sq_Ft	=''
		basement	=''
		Full_Half	=''
		Full_Bath	=''
		Half_Bath	=''
		bedrooms	=''
		Total_Value =''

	SaleDate		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[1]').text
	SalesPrice		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[2]').text
	salesValidity	=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[7]').text
	Buyer			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[8]').text
	Seller			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[9]').text

	return [[SaleDate,SalesPrice,salesValidity,Buyer,Seller],[input_key,Parcel_Number,Location_Address,Property_Class,Neighborhood,Acres,Exceptions,CurrentOwner,OwnerAddress,streetAddress,city,state,zipcode,structure,units,year_built_c,total_sq_footage,stories,construction,style,year_built_r,Res_Sq_Ft,basement,Full_Bath,Half_Bath,bedrooms,Total_Value]] 

file0			=open("sales.csv",'w')
file1			=open("property.csv",'w')
sales_writer	=csv.writer(file0)
property_writer	=csv.writer(file1)
sales_writer.writerow(salesHeader)
property_writer.writerow(propertyHeader)

'''
with Pool(processes=number_of_cpus) as pool:
	result_database=pool.map(scrape,inputs)
'''
result_database=[]

for i in range(number_of_inputs):
	result_database.append(scrape(inputs[i]))

for i in result_database:#range(len(result_database)):
	sales_writer.writerow(result_database[1][0])
	property_writer.writerow(result_database[1][1])

file0.close()
file1.close()


endingTime=time()
print("Crawling takes ( %f s ) to compete ")