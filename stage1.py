import os
import re
import zipfile
import logging
import shutil
import pandas as pd
import numpy as np
from datetime import datetime


def extractZip(config, file_path, work_dir, output_dir):
    """
        a.	Unpack the zip file
        b.	Transform the input CSV files it contains into output CSV files, none of which should be > 300MB 
        c.	Places the output CSV files into output_dir
    """
    logging.info(f"Enter in Stage1 {file_path}")
    zip_file_name = os.path.basename(file_path)
    logging.info(f"Zip Name {zip_file_name}")

    # Throw artificial error to debug error processing
    if re.search("error", zip_file_name, re.IGNORECASE):
        raise Exception("Simulated exception because the zip file name contains the word 'error'") 

    # Extract file and place in output_dir
    with zipfile.ZipFile(file_path, 'r') as f:
        f.extractall(work_dir)
    os.remove(file_path)
    """"
    # Move any CSV file to stage2_csv_dir
    for path in os.listdir(work_dir):
        full_path = os.path.join(work_dir, path)

        if re.search(r"\.csv\Z", path, re.IGNORECASE):
            shutil.move(full_path, output_dir)
    """
    return

def readFoldersAndJoin(path):
    #type_dictT = {'MANDT':'str','SPRAS':'str','PRODH':'str','VTEXT':'str'}
    #Add types
    type_dictT = {'PRODH':'str','ZZGLFUNC':'str','ZZGLVAR':'str','ZZGLBRAND':'str'}
    type_dictM = {'MATNR':'str','PRDHA':'str'}
    type_dictV = {'VBELN':'str','POSNR':'float','MATNR':'str','NETWR':'str','ERNAM':'str','KWMENG':'float','KMEIN':'str','NTGEW':'float','ABGRU':'str','KBMENG':'float','LPRIO':'str','ERDAT':'str','ERZET':'str','WERKS':'str','BRGEW':'float','GEWEI':'str','WAERK':'str','PRODH':'str'}
    files = os.listdir(path)
    #Create a list to hold the folders
    global foldersName
    foldersName = []
    for file in files:
        if file.find('.zip') == -1:
            foldersName.append(file)
    # Create a list to hold the dataframes
    df_list = []
    for folder in foldersName:
        df_list = []
        fileName = folder
        folder_path =path+'/'+folder
        files = os.listdir(folder_path)
        csv_files = [f for f in files if f.endswith('.csv')]
        for csv in csv_files:
            file_path = os.path.join(folder_path, csv)
            try:
                # Try reading the file using default UTF-8 encoding
                if fileName == 'T179T':
                    df = pd.read_csv(file_path,on_bad_lines='skip',delimiter=';',low_memory=False,dtype=type_dictT)
                    df_list.append(df)
                elif fileName == 'MARA':
                    df = pd.read_csv(file_path,on_bad_lines='skip',delimiter=';',low_memory=False,dtype=type_dictM)
                    df_list.append(df)
                elif fileName == 'VBAP':
                    df = pd.read_csv(file_path,on_bad_lines='skip',delimiter=';',low_memory=False,dtype=type_dictV)
                    df_list.append(df)
                else:
                    df = pd.read_csv(file_path,on_bad_lines='skip',delimiter=';',low_memory=False)
                    df_list.append(df)
            except UnicodeDecodeError:
                try:
                # If UTF-8 fails, try reading the file using UTF-16 encoding with tab separator
                    df = pd.read_csv(file_path, delimiter=';', encoding='utf-16')
                    df_list.append(df)
                except Exception as e:
                    raise Exception(f"Could not read file {csv} because of error: {e}")
            except Exception as e:
                raise Exception(f"Could not read file {csv} because of error: {e}")

            # Concatenate all data into one DataFrame
            big_df = pd.concat(df_list, ignore_index=True)

            # Save the final result to a new CSV file
            big_df.to_csv(os.path.join(folder_path, fileName+'.csv'), index=False)

            #Move the CSV Out
        shutil.move(os.path.join(folder_path, fileName+'.csv'),path)

    return foldersName

def createPlantMaterial(path,instransit):
    
    logging.info("Calling Plant Material")
    type_dictT = {'PRODH':'str','ZZGLFUNC':'str','ZZGLVAR':'str','ZZGLBRAND':'str'}
    type_dictM = {'MATNR':'str','PRDHA':'str'}
    #Read the CSV's to create the table
    logging.info(f"Reading plant material files in path: {path}")
    mvke = pd.read_csv(os.path.join(path,'MVKE.csv'),on_bad_lines='skip',low_memory=False)
    makt = pd.read_csv(os.path.join(path,'MAKT.csv'),on_bad_lines='skip',low_memory=False)
    t25a5 = pd.read_csv(os.path.join(path,'T25a5.csv'),on_bad_lines='skip',low_memory=False)
    t179t = pd.read_csv(os.path.join(path,'t179t.csv'),on_bad_lines='skip',low_memory=False,dtype = type_dictT)
    mara = pd.read_csv(os.path.join(path,'mara.csv'),on_bad_lines='skip',low_memory=False,dtype = type_dictM)

    
    makt = makt.drop_duplicates()
    makt = makt.drop_duplicates(subset=['MATNR'])
    if instransit:
        mvke = mvke[['MATNR','ZZ_PROD_CAT']]
        mvke = mvke.drop_duplicates()
    else:
        mvke = mvke[['MATNR','ZZ_PROD_CAT','VMSTA']]
    
    t25a5.rename(columns={'WWPRC':'ZZ_PROD_CAT'},inplace = True)
    t25a5 = t25a5.drop_duplicates()
    mara.rename(columns={'PRDHA':'PRODH'},inplace = True)
    t179t = t179t.drop_duplicates()
    t179t['ZZGLFUNC'].astype(str).replace('', np.nan, inplace=True)
    t179t.dropna(subset=['ZZGLFUNC'], inplace=True)

    mavkte_name = pd.merge(mvke,t25a5,on='ZZ_PROD_CAT',how = 'inner')
    mart = pd.merge(t179t,mara,on = 'PRODH',how = 'inner')
    mavkte = pd.merge(mart,makt,on = 'MATNR',how = 'inner')
    plantMaterial = pd.merge(mavkte,mavkte_name,on = 'MATNR',how='inner')

    plantMaterial['ProductName'] = plantMaterial.ZZ_PROD_CAT.astype(str)+' - '+plantMaterial.BEZEK
    plantMaterial = plantMaterial.drop_duplicates()
    logging.info("Done with Plant Material")
    return plantMaterial

def format_datetime(dt_series):

    def get_split_date(strdt):
        if(strdt != 0):
            str_date = strdt[4:6] + ' ' + strdt[6:8] + ' ' + strdt[:4]
            return str_date
        else:
            str_date = ''
            return str_date

    dt_series = pd.to_datetime(dt_series.apply(lambda x: get_split_date(x)), format='%m %d %Y',errors='coerce')

    return dt_series

def abgru(row):

    abgruDescription = {'0':'Assigned by the System (internal)','1':'Delivery date too late','2':'Product with Poor Quality','3':'Product Too Expensive','4':'Competitor better','5':'DO NOT USE Insufficient Guarantee','6':'Rebates CMR rejection (after CM cancel.)','7':'DO NOT USE CASA - INC Cancellation','8':'Quota Exceeded/Insufficient Allocation','9':'Order Date is Outside Promotion Validity','10':'Colgate Decision to Not Ship','11':'Cust.to receive replacement','12':'Without Valid Price','13':'UoM inconsistences/Order with decimals','14':'Obsolete Product','15':'Prom. Quota is not enough in Dist.Center','16':'COP Cancel >90 Days','20':'COP Cut due to Backorder','21':'COP Balance Cleared - Contact Customer','22':'Rejected due to Order Split','23':'Product Not Available:Supply Chain','24':'COP Canceled Line','25':'COP Custom Logo','26':'COP Cancel per A/R, Outstanding Balance','27':'COP Cancel Shipped under othr order/line','28':'COP ITB Text Block','29':'COP-Copy w/Mat Determination','30':'COP ITB Text validation','31':'COP No Credit Due','32':'COP Cancel Unshipped Product','33':'COP Product Not Returned','34':'COP Cancel due to discontinued','35':'COP Cancel OE error','36':'COP Cancel Customer error','37':'COP Mat Sub/Changed Sku','38':'COP Canceled Entire Plan','50':'DO NOT USE Transaction is being checked','55':'CMI/VMI Order Rejection','60':'Material Obsolete 1 PH','61':'Unauthorized Purchase Order','62':'Incorrect Quantity','63':'Duplicate Order','64':'Credit / Payment Issues','70':'Quota Exceeded/OOS Situation','75':'Shelf life','80':'Sales Dpt. Request','81':'Customer Requested/Approved order change','82':'NoCostEstim Release for Matl at SO Dte','83':'Error due to item with error 82','89':'Logistical disc conditions not fulfilled','90':'Wrong Order Creation.','91':'EDI: Invalid Item.','92':'EDI: Price Error.','93':'DO NOT USE Free Goods Order Increment Er','94':'DO NOT USE EDI: Invalid Deliv Date.','96':'EDI: Alt UoM Conversion Wrong.','99':'CQC - Update Error','AQ':'Minimum Order Quantity Not Met','BM':'Below Minimum Order','CC':'Customer Canceled/Changed','CO':'Close-out Item','DD':'Out of Stock due to ATP','DI':'Discontinued/Inactive/Obsolete','DS':'Master Data Integrity:Invalid Item','FS':'Ordered Outside Promotional','IM':'Master Data Integrity:Disco./Inactive','MQ':'Minimum Shipping Quantity not met','N0':'Sales Requested Change','N1':'Customer Approved Change','N4':'Pricing Issues','N5':'Insufficent Sales Allocation Quantity','OW':'Overweight order','PC':'Process Cut','PH':'Change the Old Product Hier. in SKU','PM':'Pricing Issues','QB':'Quota Block','QC':'Quota exceeded (Commercial)','QP':'Quota exceeded Promo (Commercial)','QR':'Product reserved for other customers','QS':'Quota exceeded (Short Stock)','RQ':'MOQ Reject','SL':'ST Licence Rqrd','SS':'COP Sullivan-Schein Credit Hold','TO':'Test Orders','XQ':'MOQ Cancellation','Z8':'Splitting: Order cancelled','ZH':'COP Historical Data','ZZ':'Product Not Available: Supply Chain'}

    key = row['ABGRU']
    if pd.notna(key):
        if key != '00':
            key = str(key).lstrip('0')
        else:
            key = '0'
        try:
            message = str(row['ABGRU'])+' '+str(abgruDescription[key])
        except:
            message = row['ABGRU']
        return message
    else:
        return ''
    
def cmgst(row):
    if row['CMGST'] == 'A':
        return 'A - Credit check was executed, document OK'
    elif row['CMGST'] =='B':
        return 'B - Credit check was executed, document not OK' 
    elif row['CMGST'] =='C':
        return 'C - Credit check was executed, document not OK, partial release' 
    elif row['CMGST'] =='D':
        return 'D - Document released by credit representative' 
    else:
        return 'Credit check was not executed/Status not set'

def createCurrentOrders(path,pathCSV):
    #Read the CSV's to create the table
    mydate = datetime.now()
    month = mydate.strftime("%b")
    logging.info("Calling create CurrentOrder")

    type_dictV = {'VBELN':'str','POSNR':'float','MATNR':'str','NETWR':'str','ERNAM':'str','KWMENG':'float','KMEIN':'str','NTGEW':'float','ABGRU':'str','KBMENG':'float','LPRIO':'str','ERDAT':'str','ERZET':'str','WERKS':'str','BRGEW':'float','GEWEI':'str','WAERK':'str','PRODH':'str'}
    logging.info(f"Path to read the files: {path}")
    logging.info(f"You can read the files: {os.access(path,os.R_OK)}")
    vbap = pd.read_csv(os.path.join(path,'VBAP.csv'),on_bad_lines='skip',low_memory=False,dtype=type_dictV)
    vbak = pd.read_csv(os.path.join(path,'VBAK.csv'),on_bad_lines='skip',low_memory=False)
    vbep = pd.read_csv(os.path.join(path,'VBEP.csv'),on_bad_lines='skip',low_memory=False)
    kna1 = pd.read_csv(os.path.join(path,'KNA1.csv'),on_bad_lines='skip',low_memory=False)
    knvh = pd.read_csv(os.path.join(path,'KNVH.csv'),on_bad_lines='skip',low_memory=False)

    logging.info("Was posible to read all the files")

    #Convert the values to match
    vbak['KUNNR'] = vbak['KUNNR'].astype(str)

    kna1['KUNNR'] = kna1['KUNNR'].astype(str)

    knvh['KUNNR'] = knvh['KUNNR'].astype(str)
    vbak['VBELN'] = vbak['VBELN'].astype(str)
    vbep['VBELN'] = vbep['VBELN'].astype(str)
    vbep['POSNR'] = vbep['POSNR'].astype(float)
    vbap['KWMENG'] = vbap['KWMENG'].astype(float)
    vbap['KBMENG'] = vbap['KBMENG'].astype(float)
    vbap['BRGEW'] = vbap['BRGEW'].astype(float)
    vbap['NETWR'] = vbap['NETWR'].str.replace('-','')
    vbap['NETWR'] = vbap['NETWR'].astype(float)


    vbap['ERDAT'] = format_datetime(vbap['ERDAT'])
    vbep['MBDAT'] =  vbep['MBDAT'].astype(str)
    vbep['MBDAT'] = format_datetime(vbep['MBDAT'])
    vbap['VBELN'] = vbap.VBELN.apply(lambda x: str(x).lstrip('0'))

    vbak.rename(columns={'ERDAT':'ERDAT_Item','ERZET':'ERZET_Item','ERNAM':'ERNAM_Item'},inplace = True)
    vbak['ERDAT_Item'] =  vbak['ERDAT_Item'].astype(str)
    vbak['ERDAT_Item'] = format_datetime(vbak['ERDAT_Item'])
    vbak['ZZ_VDATU'] =  vbak['ZZ_VDATU'].astype(str)
    vbak['ZZ_VDATU'] = format_datetime(vbak['ZZ_VDATU'])
    vbak['VDATU'] =  vbak['VDATU'].astype(str)
    vbak['VDATU'] = format_datetime(vbak['VDATU'])


    vbep['BMENG'] = vbep['BMENG'].astype(float)
    vbep['EDATU'] =  vbep['EDATU'].astype(str)
    vbep['EDATU'] = format_datetime(vbep['EDATU'])
    vbep['ZZ_MBDAT'] =  vbep['ZZ_MBDAT'].astype(str)
    vbep['ZZ_MBDAT'] = format_datetime(vbep['ZZ_MBDAT'])

    vbak['CMGST'] = vbak.apply(cmgst, axis = 1)
    vbap['ABGRU'] = vbap.apply(abgru, axis = 1)

    vbap['SD header creation date'] = vbap['ERDAT']
    knvh2 = knvh[knvh['KUNNR'].duplicated()]
    knvh2.rename(columns = {'HKUNNR':'Level2'},inplace = True)
    knvh = knvh.drop_duplicates(subset=['KUNNR'],keep='last')
    knvhf = pd.merge(knvh,knvh2,on = 'KUNNR',how='left')
    #knvhf = knvh
    knvhf.drop_duplicates(subset=['KUNNR'],keep='first')

    logging.info("Start to join tables")

    #join all the tables
    plantMaterial = createPlantMaterial(path,False)
    vbapk = pd.merge(vbap,vbak,on ='VBELN',how = 'inner')
    vbapk = vbapk.drop_duplicates()
    vbepk = pd.merge(vbapk,vbep,on = ['VBELN','POSNR'],how = 'inner')
    vbepk = vbepk.drop_duplicates()
    currentOrder = pd.merge(vbepk,kna1,on = 'KUNNR',how = 'inner')
    currentOrder = currentOrder.drop_duplicates()
    #Create new Fields
    #Create Id
    currentOrder['Id'] = currentOrder.VBELN.astype(str)+'-'+currentOrder.POSNR.astype(str)

    #Create Activity
    currentOrder['Activity'] = 'CurrentOrder'
    currentOrder['Sold To Party Name'] = currentOrder.KUNNR.astype(str)+'-'+currentOrder.NAME1.astype(str)
    #Create MaterialCuts
    pivot = currentOrder.loc[currentOrder['ETENR'] == 1,['Id','KWMENG']]
    tmp = currentOrder[['Id','BMENG']]
    tmp1 = tmp.groupby(by = ["Id"]).sum()
    pivot = pd.merge(pivot,tmp1,on ='Id',how = 'inner')
    pivot['MaterialCuts1'] = pivot['KWMENG'] - pivot['BMENG']
    pivot.drop(columns=['BMENG','KWMENG'],axis =1,inplace = True)
    currentOrder = pd.merge(currentOrder,pivot,on = ['Id'],how = 'inner')
    currentOrder = currentOrder.drop_duplicates()
    currentOrder['MaterialCuts2'] = np.where(np.logical_and(currentOrder['MaterialCuts1'] == 0,currentOrder['MaterialCuts1'] !=0),currentOrder['NETWR'],((currentOrder.KWMENG - currentOrder.MaterialCuts1)*(currentOrder.NETWR/currentOrder.KWMENG)))
    currentOrder['MaterialCuts2'] = round(currentOrder.MaterialCuts2,2)
    currencyF = currentOrder['WAERK'].unique()

    logging.info("Before Calling currency Function")
    for f in currencyF:
        if f != 'USD':
            currency = float(getCurrencyChange(path,f))
            currentOrder['MaterialCuts3'] =   np.where(currentOrder.WAERK == f,round((currentOrder.MaterialCuts2/currency),2),currentOrder.MaterialCuts2)
            currentOrder['NETWR_CONVERTED'] = np.where(currentOrder.WAERK == f,round((currentOrder.NETWR/currency),2),currentOrder.NETWR)
        else:
            currentOrder['MaterialCuts3'] = currentOrder.MaterialCuts2
            currentOrder['NETWR_CONVERTED'] = currentOrder.NETWR


    #conf_total_quantity
    today = datetime.now()
    todaynp = np.datetime64(today)
    currentOrder['conf_total_quantity'] = np.where(np.logical_and(currentOrder['BMENG'] > currentOrder['KBMENG'],currentOrder['BMENG']/2 == currentOrder['KBMENG'],currentOrder['EDATU'] >= todaynp),currentOrder['BMENG'],currentOrder['KBMENG'])

    #unconf_total_quantity
    currentOrder['unconf_total_quantity'] = currentOrder.KWMENG - currentOrder.conf_total_quantity

    #confirmed_quantity_weight
    currentOrder['confirmed_quantity_weight'] = round((currentOrder.BRGEW/currentOrder.KWMENG)*currentOrder.KBMENG,1)

    currentOrder['Weight'] = currentOrder.BRGEW
    
    currentOrder = pd.merge(currentOrder,knvhf,on = 'KUNNR',how = 'inner')
    currentOrder = pd.merge(currentOrder,plantMaterial,on = ['MATNR','PRODH'],how = 'inner')
    currentOrder['MATNR'] = currentOrder.MATNR.apply(lambda x: str(x).lstrip('0'))
    currentOrder = currentOrder.drop_duplicates()
    currentOrder.loc[currentOrder['ETENR'] != 1, ['Activity']] = 'Delivery Schedule Line'
    currentOrder['Type'] = 'Sales Order'

    currentOrder = currentOrder[['ABGRU','Activity','AUART','BRGEW','BSTNK','CMGST','conf_total_quantity','confirmed_quantity_weight','ERDAT','ERDAT_Item','ETENR','GEWEI','Id','KBMENG','KMEIN','KUNNR','KWMENG','LIFSK','MAKTX','MaterialCuts1','MaterialCuts2','MaterialCuts3','MATNR','MBDAT','NAME1','NETWR','NETWR_CONVERTED','POSNR','ProductName','SD header creation date','Sold To Party Name','Type','unconf_total_quantity','VBELN','VBTYP','VDATU','VKORG','WAERK','WERKS','WMENG','ZZ_VDATU','ZZGLFUNC','ZZORATE']]

    rows = len(currentOrder)
    currentOrder = pd.concat([currentOrder]*2, ignore_index=True)
    id = currentOrder.iloc[rows:].WERKS+'-'+currentOrder.iloc[rows:].MATNR
    currentOrder['Id'].iloc[rows:] = id
    currentOrder['Type'].iloc[rows:] = 'Plant Material'

    currentOrder = currentOrder.loc[((currentOrder['Activity'] != 'Delivery Schedule Line') & ( currentOrder['ETENR'] == 1 )) 
    | ((currentOrder['Activity'] == 'Delivery Schedule Line') & ( currentOrder['ETENR'] != 1 ))]

    fileName = f'CurrentOrder_{month}'
    write_in_chunks(currentOrder,'CurrentOrder.csv',fileName,pathCSV)

def createInstansitItems(path,pathCSV):
    mydate = datetime.now()
    month = mydate.strftime("%b")
    logging.info("Calling create IntransitItem")
    logging.info(f"Merge Path: {path}")
    logging.info(f"Final Path: {pathCSV}")
    #month = 'Nov'
    #type_dictV = {'VBELN':'str','POSNR':'float','MATNR':'str','NETWR':'str','ERNAM':'str','KWMENG':'float','KMEIN':'str','NTGEW':'float','ABGRU':'str','KBMENG':'float','LPRIO':'str','ERDAT':'str','ERZET':'str','WERKS':'str','BRGEW':'float','GEWEI':'str','WAERK':'str','PRODH':'str'}

    vbap = pd.read_csv(os.path.join(path,'VBAP.csv'),on_bad_lines='skip',low_memory=False)
    lips = pd.read_csv(os.path.join(path,'LIPS.csv'),on_bad_lines='skip',low_memory=False)
    likp = pd.read_csv(os.path.join(path,'LIKP.csv'),on_bad_lines='skip',low_memory=False)
    ekpo = pd.read_csv(os.path.join(path,'EKPO.csv'),on_bad_lines='skip',low_memory=False)
    vttk = pd.read_csv(os.path.join(path,'VTTK.csv'),on_bad_lines='skip',low_memory=False)
    vttp = pd.read_csv(os.path.join(path,'VTTP.csv'),on_bad_lines='skip',low_memory=False)
    mard = pd.read_csv(os.path.join(path,'MARD.csv'),on_bad_lines='skip',low_memory=False)
    eket = pd.read_csv(os.path.join(path,'EKET.csv'),on_bad_lines='skip',low_memory=False)
    plaf = pd.read_csv(os.path.join(path,'PLAF.csv'),on_bad_lines='skip',low_memory=False)
    #Clean VBAP
    vbap = vbap[['VBELN','POSNR','KWMENG','NETWR']]
    
    #Rename Columns
    vttk.rename(columns={'ERDAT':'ERDAT_vttk'},inplace = True)
    vbap.rename(columns={'POSNR':'VGPOS','VBELN':'VGBEL'},inplace = True)
    ekpo.rename(columns={'MATNR':'MATNR_Ekpo','WERKS':'WERKS_Ekpo','LGORT':'LGORT_Ekpo'},inplace = True)
    plaf.rename(columns= {'PLWRK':'WERKS'},inplace = True)
    vbap['VGPOS'] = vbap['VGPOS'].astype(float)
    lips['VGPOS'] = lips['VGPOS'].astype(float)

    #format Date
    vttk['ERDAT_vttk'] = vttk['ERDAT_vttk'].astype(str)
    vttk['ERDAT_vttk'] =format_datetime(vttk['ERDAT_vttk'])
    eket['EINDT'] = eket['EINDT'].astype(str)
    eket['EINDT'] = format_datetime(eket['EINDT'])
    lips['MBDAT'] = lips['MBDAT'].astype(str)
    lips['MBDAT'] = format_datetime(lips['MBDAT'])
    lips['ERDAT'] = lips['ERDAT'].astype(str)
    lips['ERDAT'] = format_datetime(lips['ERDAT'])
    likp['LFDAT'] = likp['LFDAT'].astype(str)
    likp['LFDAT'] = format_datetime(likp['LFDAT'])
    likp['WADAT'] = likp['WADAT'].astype(str)
    likp['WADAT'] = format_datetime(likp['WADAT'])
    likp['WADAT_IST'] = likp['WADAT_IST'].astype(str)
    likp['WADAT_IST'] = format_datetime(likp['WADAT_IST'])
    likp['ZZACTDLDAT'] = likp['ZZACTDLDAT'].astype(str)
    likp['ZZACTDLDAT'] =  format_datetime(likp['ZZACTDLDAT'])
    vttk['DPREG']= vttk['DPREG'].astype(str)
    vttk['DPREG'] = format_datetime(vttk['DPREG'])
    vttk['DAREG']= vttk['DAREG'].astype(str)
    vttk['DAREG'] = format_datetime(vttk['DAREG'])
    vttk['DPABF']= vttk['DPABF'].astype(str)
    vttk['DPABF'] = format_datetime(vttk['DPABF'])
    vttk['DTABF']= vttk['DTABF'].astype(str)
    vttk['DTABF'] = format_datetime(vttk['DTABF'])

    #Start Joins
    lipk = pd.merge(lips,likp,on = 'VBELN',how ='inner')

    vlip = pd.merge(lipk,vbap,on = ['VGPOS','VGBEL'],how ='left')
 
    ekot = pd.merge(ekpo,eket,on = ['EBELN','EBELP'],how = 'inner')

    ekot.rename(columns={'EBELN':'VGBEL','EBELP':'VGPOS'},inplace = True)
    vlek = pd.merge(vlip,mard,on = ['WERKS','LGORT','MATNR'],how = 'inner')

    vlim = pd.merge(vlek,ekot,on = ['VGBEL','VGPOS'],how = 'left')

    vttpk = pd.merge(vttp,vttk,on = 'TKNUM',how='inner')

    vttl = pd.merge(vlim,vttpk,on= 'VBELN',how ='inner')
 
    plt = pd.merge(vttl,plaf,on= ['MATNR','WERKS'],how ='left')

    plantMaterial = createPlantMaterial(path,True)
    intransitItem = pd.merge(plt,plantMaterial,on = 'MATNR',how = 'inner')
    intransitItem['LABST'] = intransitItem['LABST'].astype(str).str.replace('-','')
    intransitItem['LABST'] = intransitItem['LABST'].astype(float)
    intransitItem['MATNR_Ekpo'] = intransitItem.MATNR_Ekpo.apply(lambda x: str(x).lstrip('0'))
    intransitItem['MATNR'] = intransitItem.MATNR.apply(lambda x: str(x).lstrip('0'))

    intransitItem['On_order_kpi'] = np.where(intransitItem.LGMNG != 0,intransitItem.LGMNG,intransitItem.MENGE)
    intransitItem['sum_of_req_quantity_mat_kpi'] = intransitItem.KWMENG.astype(float)
    intransitItem['qty_on_deliveries'] = intransitItem.KWMENG.astype(float)
    intransitItem['available_after_open_orders'] = intransitItem.LABST.astype(float)
    intransitItem['net_inventory_available']= intransitItem.LABST.astype(float)+intransitItem.On_order_kpi.astype(float)-intransitItem.LGMNG.astype(float)

    intransitItem['Activity'] = 'Instransit Item'
    intransitItem['Id'] = intransitItem.WERKS+'-'+intransitItem.MATNR
    intransitItem['Type'] = 'Total Plant'
    intransitItem =  intransitItem[['Activity','available_after_open_orders','BRGEW','CHARG','DPREG','EINDT','EINME','ERDAT_vttk','ERDAT_y','GSMNG','Id','INSME','LABST','LFDAT','LGMNG','LGORT','LGORT_Ekpo','MAKTX','MATNR','MBDAT','net_inventory_available','On_order_kpi','POSNR','ProductName','SPEME','STTRG','TKNUM','Type','UMLME','VBELN','VSTEL','WADAT','WERKS','ZZGLFUNC']]
    rows = len(intransitItem)
    intransitItem = pd.concat([intransitItem]*3,ignore_index=True)
    id = intransitItem.iloc[rows:rows*2].WERKS+'-'+intransitItem.iloc[rows:rows*2].MATNR+'-'+intransitItem.iloc[rows:rows*2].LGORT
    intransitItem['Id'].iloc[rows:rows*2] = id
    intransitItem['Type'].iloc[rows:rows*2] = 'Storage Location'

    id = intransitItem.iloc[rows*2:].VBELN.astype(str)+'-'+intransitItem.iloc[rows*2:].POSNR.astype(str)+'-'+intransitItem.iloc[rows*2:].TKNUM.astype(str)
    intransitItem['Id'].iloc[rows*2:] = id
    intransitItem['Type'].iloc[rows*2:] = 'Instransit'
    fileName = f'IntransitItem_{month}'
    write_in_chunks(intransitItem,'IntransitItem.csv',fileName,pathCSV)
    
def write_in_chunks(df, file_name,file0,path):
    lines_per_file = 80000
    file = os.path.join(path,file_name)
    logging.info(f"File Name: {file}")
    df.to_csv(file,index = False)
    logging.info(f"Starting to move create the little CSVs")

    with open(file, 'r', encoding='utf-8') as file:
        logging.info(f'1### Step')
        header = file.readline()  # read the header
        file_num = 1        
        while True:
            logging.info(f'2### Step')
            lines = [file.readline() for _ in range(lines_per_file)]
            if not lines[0]:  # stop when end of file is reached
                break
            fileName = os.path.join(path,f'{file0}_{file_num}.csv')
            logging.info(f'3### {file0}_{file_num}.csv')
            with open(fileName, 'w', encoding='utf-8') as out_file:
                out_file.write(header)  # write the header to each output file
                out_file.writelines(lines)
            file_num += 1
    os.remove(path+'/'+file_name)
    


def getCurrencyChange(path,fromC):
    if fromC != 'USD':
        tcurr = pd.read_csv(os.path.join(path,'TCURR.csv'),on_bad_lines='skip',low_memory=False)
        tcurr['UKURS'] = tcurr['UKURS'].str.replace('-','')
        tcurr = tcurr.loc[tcurr['FCURR'] == fromC,['KURST','TCURR','GDATU','UKURS']]
        value = tcurr.values[:1]
        if len(value) > 0:
            return value[0][3]
        else:
            return 18
    else:
        return 1

def deleteData(path,foldersName):
    for file in foldersName:
        os.remove(os.path.join(path, file+'.csv'))
        shutil.rmtree(os.path.join(path, file))

def createDigitalTransformation(path,pathCSV):
    mydate = datetime.now()
    #month = mydate.strftime("%d_%b")
    month = 'Nov'

    vbap = pd.read_csv(os.path.join(path,'VBAP.csv'),on_bad_lines='skip',low_memory=False)
    vbak = pd.read_csv(os.path.join(path,'VBAK.csv'),on_bad_lines='skip',low_memory=False)
    vbep = pd.read_csv(os.path.join(path,'VBEP.csv'),on_bad_lines='skip',low_memory=False)
    lips = pd.read_csv(os.path.join(path,'LIPS.csv'),on_bad_lines='skip',low_memory=False)
    likp = pd.read_csv(os.path.join(path,'LIKP.csv'),on_bad_lines='skip',low_memory=False)
    kna1 = pd.read_csv(os.path.join(path,'KNA1.csv'),on_bad_lines='skip',low_memory=False)
    marm = pd.read_csv(os.path.join(path,'MARM.csv'),on_bad_lines='skip',low_memory=False)
    #t001 = pd.read_csv(os.path.join(path,'T001.csv'),on_bad_lines='skip',low_memory=False)
    usr2 = pd.read_csv(os.path.join(path,'USR02.csv'),on_bad_lines='skip',low_memory=False)
    
    #Clean Tables
    vbap = vbap[['VBELN','POSNR','NETWR','ERDAT','KWMENG','MATNR','WERKS','WAERK','ABGRU','KMEIN']]
    vbak = vbak[['VBELN','KUNNR','ERDAT','ERNAM','ZZ_VDATU','VDATU','VKORG','AUART','LIFSK','ABHOD','VBTYP','BUKRS_VF']]
    vbep = vbep[['VBELN','ETENR','POSNR','WADAT','EDATU','BMENG','MBDAT']]
    lips = lips[['VBELN','POSNR','VGBEL','VGPOS','LGMNG','LFIMG','ERDAT',]]
    likp = likp[['VBELN','ZZACTDLDAT','WADAT_IST','ERDAT','WADAT']]

    #Format Fields
    vbak['KUNNR'] = vbak['KUNNR'].astype(str)
    kna1['KUNNR'] = kna1['KUNNR'].astype(str)
    vbap['NETWR'] = vbap['NETWR'].str.replace('-','')
    vbap['NETWR'] = vbap['NETWR'].astype(float)

    #Rename Fields
    vbap.rename(columns={'ERDAT':'ERDAT_Vbap'},inplace = True)
    vbak.rename(columns={'ERDAT':'ERDAT_Vbak','ERNAM':'ERNAM_Item','BUKRS_VF':'BUKRS','AUART':'Sales Order Type'},inplace = True)
    lips.rename(columns={'ERDAT':'ERDAT_Lips'},inplace = True)
    likp.rename(columns={'ERDAT':'ERDAT_Likp','WADAT':'WADAT_Likp','ZZACTDLDAT':'POD'},inplace = True)
    usr2.rename(columns={'BNAME':'ERNAM_Item'},inplace = True)
    kna1.rename(columns={'NAME1':'Costumer'},inplace = True)
    marm.rename(columns={'MEINH':'KMEIN'},inplace = True)

    vbap['Activity'] = 'Create Sales Order'

    #Format Dates
    vbap['ERDAT_Vbap'] = vbap['ERDAT_Vbap'].astype(str)
    vbap['ERDAT_Vbap'] = format_datetime(vbap['ERDAT_Vbap'])
    vbak['ERDAT_Vbak'] = vbak['ERDAT_Vbak'].astype(str)
    vbak['ERDAT_Vbak'] = format_datetime(vbak['ERDAT_Vbak'])
    lips['ERDAT_Lips'] = lips['ERDAT_Lips'].astype(str)
    lips['ERDAT_Lips'] = format_datetime(lips['ERDAT_Lips'])
    likp['ERDAT_Likp'] = likp['ERDAT_Likp'].astype(str)
    likp['ERDAT_Likp'] = format_datetime(likp['ERDAT_Likp'])
    vbak['ABHOD'] = vbak['ABHOD'].astype(str)
    vbak['ABHOD'] = format_datetime(vbak['ABHOD'])
    vbep['EDATU'] = vbep['EDATU'].astype(str)
    vbep['EDATU'] = format_datetime(vbep['EDATU'])
    vbep['MBDAT'] = vbep['MBDAT'].astype(str)
    vbep['MBDAT'] = format_datetime(vbep['MBDAT'])
    vbep['WADAT'] = vbep['WADAT'].astype(str)
    vbep['WADAT'] = format_datetime(vbep['WADAT'])
    vbak['ZZ_VDATU'] = vbak['ZZ_VDATU'].astype(str)
    vbak['ZZ_VDATU'] = format_datetime(vbak['ZZ_VDATU'])
    vbak['VDATU'] = vbak['VDATU'].astype(str)
    vbak['VDATU'] = format_datetime(vbak['VDATU'])
    likp['WADAT_Likp'] = likp['WADAT_Likp'].astype(str)
    likp['WADAT_Likp'] = format_datetime(likp['WADAT_Likp'])
    likp['WADAT_IST'] = likp['WADAT_IST'].astype(str)
    likp['WADAT_IST'] = format_datetime(likp['WADAT_IST'])
    likp['POD'] = likp['POD'].astype(str)
    likp['POD'] = format_datetime(likp['POD'])

    lips['ts_ERDAT_Lips'] = lips['ERDAT_Lips'].apply(lambda dt: dt.replace(day=1))
    vbap['ts_ERDAT'] = vbap['ERDAT_Vbap'].apply(lambda dt: dt.replace(day=1))
    
    #Currency
    currencyF = vbap['WAERK'].unique()
    for f in currencyF:
        if f != 'USD':
            currency = float(getCurrencyChange(path,f))
            vbap['NETWR_CONVERTED'] = np.where(vbap.WAERK == f,round((vbap.NETWR/currency),2),vbap.NETWR)
        else:
            vbap['NETWR_CONVERTED'] = vbap.NETWR

    #join tables
    vbapk = pd.merge(vbap,vbak,on ='VBELN',how = 'inner')
    vbepk = pd.merge(vbapk,vbep,on = ['VBELN','POSNR'],how = 'inner')
    vbepkn = pd.merge(vbepk,kna1,on = 'KUNNR',how = 'inner')
    vbmrm = pd.merge(vbepkn,marm,on = ['MATNR','KMEIN'],how = 'inner')
    vbuser = pd.merge(vbmrm,usr2,on = 'ERNAM_Item',how = 'inner')
    vbuser.rename(columns={'POSNR':'VGPOS','VBELN':'VGBEL'},inplace = True)
    lipks = pd.merge(lips,likp,on = 'VBELN',how ='inner')
    lipks = lipks.drop(lipks[(lipks['LGMNG'] == 0) & (lipks['LFIMG'] == 0)].index)
    vbuser.loc[vbuser['ETENR'] != 1, ['Activity']] = 'Delivery Schedule Line'

    digital1 = pd.merge(vbuser,lipks,on = ['VGPOS','VGBEL'],how ='left')
    digital1['VGBEL'] = digital1['VGBEL'].astype(float)
    digital1['ETENR'] = digital1['ETENR'].astype(float)
    digital1['UMREZ'] = digital1['UMREZ'].astype(float)
    digital1['UMREN'] = digital1['UMREN'].astype(float)
    digital1['timeStamp'] = digital1.ERDAT_Vbap

    digital1['MATNR'] = digital1.MATNR.apply(lambda x: str(x).lstrip('0'))

    rows = len(digital1)
    digital1 = pd.concat([digital1]*6,ignore_index=True)
    digital1['Activity'].iloc[rows:rows*2] = 'Sales Order Item Creation'
    digital1['timeStamp'].iloc[rows:rows*2] = digital1['ERDAT_Vbak'].iloc[rows:rows*2]
    digital1['Activity'].iloc[rows*2:rows*3] = 'Record good issue'
    digital1['timeStamp'].iloc[rows*2:rows*3] = digital1['WADAT'].iloc[rows*2:rows*3]
    digital1['Activity'].iloc[rows*3:rows*4] = 'Material Availability'
    digital1['timeStamp'].iloc[rows*3:rows*4] = digital1['MBDAT'].iloc[rows*3:rows*4]
    digital1['Activity'].iloc[rows*4:rows*5] = 'Requested Delivery'
    digital1['timeStamp'].iloc[rows*4:rows*5] = digital1['VDATU'].iloc[rows*4:rows*5]
    digital1['Activity'].iloc[rows*5:] = 'Original Delivery'
    digital1['timeStamp'].iloc[rows*5:] = digital1['ZZ_VDATU'].iloc[rows*5:]
    
    digital1['timeStamp'].astype(str).replace('', np.nan, inplace=True)
    digital1.dropna(subset=['timeStamp'], inplace=True)

    digital1 = digital1.loc[((digital1['Activity'] != 'Delivery Schedule Line') & ( digital1['ETENR'] == 1 )) 
            | ((digital1['Activity'] == 'Delivery Schedule Line') & ( digital1['ETENR'] != 1 ))]

    #vbuserE = vbuser.loc[vbuser['ETENR'] == 1 ]
    digital2 = pd.merge(vbuser,lipks,on = ['VGPOS','VGBEL'],how ='right')
    digital2['VBELN'] = digital2['VBELN'].astype(float)
    digital2['POSNR'] = digital2['POSNR'].astype(float)
    digital2['Activity'] = 'Create Delivery'
    digital2['timeStamp'] = digital2.ERDAT_Likp
    digital2['VGBEL'].replace('', np.nan, inplace=True)
    digital2.dropna(subset=['VGBEL'], inplace=True)
    digital2['MATNR'] = digital2.MATNR.apply(lambda x: str(x).lstrip('0'))
    rows = len(digital2)
    
    digital2 = pd.concat([digital2]*3,ignore_index=True)
    #digital2['Activity'].iloc[rows:rows*2] = 'Create Delivery Item'
    #digital2['timeStamp'].iloc[rows:rows*2] = digital2['ERDAT_Lips'].iloc[rows:rows*2]
    digital2['Activity'].iloc[rows:rows*2] = 'Proof of Delivery'
    digital2['timeStamp'].iloc[rows:rows*2] = digital2['POD'].iloc[rows:rows*2]
    #digital2['Activity'].iloc[rows*3:rows*4] = 'Planned Goods Movement'
    #digital2['timeStamp'].iloc[rows*3:rows*4] = digital2['WADAT_Likp'].iloc[rows*3:rows*4]
    digital2['Activity'].iloc[rows*2:] = 'Actual Goods Movement'
    digital2['timeStamp'].iloc[rows*2:] = digital2['WADAT_IST'].iloc[rows*2:]
    digital2['timeStamp'].astype(str).replace('', np.nan, inplace=True)
    digital2.dropna(subset=['timeStamp'], inplace=True)
    digital2 = digital2.loc[((digital2['Activity'] != 'Delivery Schedule Line') & ( digital2['ETENR'] == 1 )) 
            | ((digital2['Activity'] == 'Delivery Schedule Line') & ( digital2['ETENR'] != 1 ))]

    digital = pd.concat([digital1,digital2],ignore_index=True)
    digital['Id'] = digital.VGBEL.astype(str)+'-'+digital.VGPOS.astype(str)
    pivot = digital[['Id','LGMNG','LFIMG','Activity']]
    digital.drop(columns=['LGMNG','LFIMG','POSNR'],inplace=True)
    pivot = pivot.groupby(['Id','Activity'],as_index=False)[['LGMNG','LFIMG']].sum()
    digital = digital.drop_duplicates()
    digital = pd.merge(digital,pivot,on = ['Id','Activity'],how = 'left')

    fileName = f'Digital_{month}'
    write_in_chunks(digital,'Digital.csv',fileName,pathCSV)