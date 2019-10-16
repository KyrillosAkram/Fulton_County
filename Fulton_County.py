from time import time
from bs4 import BeautifulSoup
from urllib import urlopen
import csv
from multiprocessing import cpu_count,Pool
startTime=time()
inputs_path		="inputs.txt"
propertyHeader	=['Input','ParcelID','Location Address','PropertyType','Neighborhood','Acreage','Exemption Codes','CurrentOwner','OwnerAddress','city','state','zipcode','structure','Units','Year Built','Sq ft','stories','construction','style','Year built','Sq Ft','basement','Full Bath','Half Bath','bedrooms','value']
salesHeader		=['SalesDate','SalesPrice','SalesType','Buyer','Seller']
sales_writer	=csv.writer(open("sales.csv",'w'))
property_writer	=csv.writer(open("property.csv",'w'))
sales_writer.writerow(salesHeader)
property_writer.writerow(propertyHeader)
inputs			=[line.strip() for line in open(inputs_path,'r')]
sales_writer.close()
property_writer.close()
number_of_inputs=len(inputs)
current			=0
number_of_cpus	=cpu_count()
print("found %i cpu and will be used to accelerate the proccess")

result_database=[]

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

#keyvalue =decodekeyvalue(inputData)
#print(keyvalue)

def scrape(key):
	input_key		=key
	key				=decodekeyvalue(key)
	url				="https://qpublic.schneidercorp.com/Application.aspx?AppID=936&LayerID=18251&PageTypeID=4&PageID=8156&Q=1271885427&KeyValue="+key
	print("[%0.3i]\tscraping %s"%(current,input_key))
	current			=current+1
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

	SaleDate		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody').findNext('td').get_text()
	SalesPrice		=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody').findNext('td').findNext('td').get_text()
	salesValidity	=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[7]').get_text()
	Buyer			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[8]').get_text()
	Seller			=page.xpath('/html/body/form/div[5]/div/div[1]/main/section[5]/div/div/table/tbody/tr/td[9]').get_text()

	return [[SaleDate,SalesPrice,salesValidity,Buyer,Seller],[input_key,Parcel_Number,Location_Address,Property_Class,Neighborhood,Acres,Exceptions,CurrentOwner,OwnerAddress,streetAddress,city,state,zipcode,structure,units,year_built_c,total_sq_footage,stories,construction,style,year_built_r,Res_Sq_Ft,basement,Full_Bath,Half_Bath,bedrooms,Total_Value]] 

with Pool(processes=number_of_cpus) as pool:
	result_database=pool.map(scrape,inputs)

sales_writer.writerow([SaleDate,SalesPrice,salesValidity,Buyer,Seller])
property_writer.writerow([input_key,Parcel_Number,Location_Address,Property_Class,Neighborhood,Acres,Exceptions,CurrentOwner,OwnerAddress,streetAddress,city,state,zipcode,structure,units,year_built_c,total_sq_footage,stories,construction,style,year_built_r,Res_Sq_Ft,basement,Full_Bath,Half_Bath,bedrooms,Total_Value])

endingTime=time()
print("Crawling takes ( %f s ) to compete ")