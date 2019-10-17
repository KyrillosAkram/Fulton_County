from time import time
from bs4 import BeautifulSoup
from requests import get
import csv
from multiprocessing import cpu_count,Pool
startTime=time()
use_all_cores=True if input("Do you want to use all cores to accelereta scraping ?\t(y/n)\n\t")=='y' else False

inputs_path		='inputs.txt'
propertyHeader	=['Input','ParcelID','Location Address','PropertyType','Neighborhood','Acreage','Exemption Codes','CurrentOwner','OwnerAddress','city','state','zipcode','structure','Units','Year Built','Sq ft','stories','construction','style','Year built','Sq Ft','basement','Full Bath','Half Bath','bedrooms','value']
salesHeader		=['SalesDate','SalesPrice','SalesType','Buyer','Seller']
file2			=open(inputs_path,'r')
inputs			=[line.strip() for line in file2]
file2.close()
number_of_inputs=len(inputs)
if use_all_cores:
	number_of_cpus	=cpu_count()
	print('found %i cpu and will be used to accelerate the proccess'%number_of_cpus)
else:
	print('single core mood activated')
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
	if (lL!=-1): 
		keyvalue=keyvalue[0:lL]+'++'+keyvalue[lL:]
	ll=keyvalue.find('l',2)
	if (ll!=-1): 
		keyvalue=keyvalue[0:ll]+'++'+keyvalue[ll:]
	if (inputdata[0]=='1'):
		keyvalue=keyvalue[0:2]+'+'+keyvalue[2:]
	return keyvalue



def scrape(key):
	input_key		=key
	keyval			=decodekeyvalue(input_key)
	url				='https://qpublic.schneidercorp.com/Application.aspx?AppID=936&LayerID=18251&PageTypeID=4&PageID=8156&Q=1271885427&KeyValue='+keyval
	print('scraping %s\n'%(input_key))
	response		=get(url).text
	page			=BeautifulSoup(response,'lxml')
	Parcel_Number   =page.find('strong',text='Parcel Number').findParent('td').nextSibling.nextSibling.text.strip()
	Location_Address=page.find('strong',text='Location Address').findParent('td').nextSibling.nextSibling.text.strip()
	Property_Class  =page.find('strong',text='Property Class').findParent('td').nextSibling.nextSibling.text.strip()
	Neighborhood    =page.find('strong',text='Neighborhood').findParent('td').nextSibling.nextSibling.text.strip()
	Acres           =page.find('strong',text='Acres').findParent('td').nextSibling.nextSibling.text.strip()
	Exceptions      =page.find('strong',text='Exemptions').findParent('td').nextSibling.nextSibling.text.strip()
	OwnerAddress	=page.find('span',{'id':'ctlBodyPane_ctl01_ctl01_lblAddress1'}).text.strip().split(' ')
	streetAddress	=page.find('span',{'id':'ctlBodyPane_ctl01_ctl01_lblCityStZip'}).text.strip()
	CurrentOwner	=page.find('span',{'id':'ctlBodyPane_ctl01_ctl01_lnkOwnerName1_lblSearch'})
	CurrentOwner	=CurrentOwner.text.strip() if CurrentOwner else page.find('a',{'id':'ctlBodyPane_ctl01_ctl01_lnkOwnerName1_lnkSearch'}).text.strip()
	city			=streetAddress.pop(0)
	if streetAddress[0]=='CITY':
		streetAddress.remove('CITY')
	state			=streetAddress.pop(0)
	for textx in OwnerAddress:
		textx=textx+' '
	zipcode			=''.join(OwnerAddress)
	structure		=[]
	units			=[]
	year_built_c	=[]
	total_sq_footage=[]
	if(page.find('h1',{'id':'ctlBodyPane_ctl04_lblName'})):
		num=0
		for item in page.find('section',{'id':'ctlBodyPane_ctl04_mSection'}).findAll('strong',text='Structure'):
			structure.append(item.findParent('td').nextSibling.nextSibling.text.strip())
		for item in page.find('section',{'id':'ctlBodyPane_ctl04_mSection'}).findAll('strong',text='Units'):
			units.append(item.findParent('td').nextSibling.nextSibling.text.strip())
		for item in page.find('section',{'id':'ctlBodyPane_ctl04_mSection'}).findAll('strong',text='Year Built'):
			year_built_c.append(item.findParent('td').nextSibling.nextSibling.text.strip())
		for item in page.find('section',{'id':'ctlBodyPane_ctl04_mSection'}).findAll('strong',text='Total Sq Footage'):
			total_sq_footage.append(item.findParent('td').nextSibling.nextSibling.text.strip())
		
	if(page.find('h1',{'id':'ctlBodyPane_ctl03_lblName'})):
		stories		=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Stories').findParent('td').nextSibling.nextSibling.text.strip()
		construction=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Exterior Wall').findParent('td').nextSibling.nextSibling.text.strip()
		style		=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Style').findParent('td').nextSibling.nextSibling.text.strip()
		year_built_r=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Year Built').findParent('td').nextSibling.nextSibling.text.strip()
		Res_Sq_Ft	=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Res Sq Ft').findParent('td').nextSibling.nextSibling.text.strip()
		basement	=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Basement').findParent('td').nextSibling.nextSibling.text.strip()
		Full_Half	=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Full Bath/Half Bath').findParent('td').nextSibling.nextSibling.text.strip().split('/')
		Full_Bath	=Full_Half[0]
		Half_Bath	=Full_Half[1]
		bedrooms	=page.find('section',{'id':'ctlBodyPane_ctl03_mSection'}).findNext('strong',text='Bedrooms').findParent('td').nextSibling.nextSibling.text.strip()
		Total_Value	=page.find('td',text='Total Value').findNext('td').text.strip()
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
	salesTable		=page.select_one('#ctlBodyPane_ctl06_ctl01_gvwSales').tbody
	SaleDate		=salesTable.findNext('tr').findChildren('td',recursive=False)[0].text.strip()
	SalesPrice		=salesTable.findNext('tr').findChildren('td',recursive=False)[1].text.strip()
	salesValidity	=salesTable.findNext('tr').findChildren('td',recursive=False)[6].text.strip()
	Buyer			=salesTable.findNext('tr').findChildren('td',recursive=False)[7].text.strip()
	Seller			=salesTable.findNext('tr').findChildren('td',recursive=False)[8].text.strip()

	return [[SaleDate,SalesPrice,salesValidity,Buyer,Seller],[input_key,Parcel_Number,Location_Address,Property_Class,Neighborhood,Acres,Exceptions,CurrentOwner,OwnerAddress,streetAddress,city,state,zipcode,structure,units,year_built_c,total_sq_footage,stories,construction,style,year_built_r,Res_Sq_Ft,basement,Full_Bath,Half_Bath,bedrooms,Total_Value]] 

file0			=open('sales.csv','w')
file1			=open('property.csv','w')
sales_writer	=csv.writer(file0)
property_writer	=csv.writer(file1)
sales_writer.writerow(salesHeader)
property_writer.writerow(propertyHeader)

if use_all_cores:
	
	with Pool(processes=number_of_cpus) as pool:
		result_database=pool.map(scrape,inputs)

else:
	result_database=[]
	for i in range(number_of_inputs):
		result_database.append(scrape(inputs[i]))

for i in range(number_of_inputs):
	sales_writer.writerow(result_database[i][0])
	property_writer.writerow(result_database[i][1])



file0.close()
file1.close()


endingTime=time()
print('Crawling takes ( %f s ) to complete '%(endingTime-startTime))