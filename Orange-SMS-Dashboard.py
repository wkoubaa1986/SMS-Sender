# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 10:18:44 2023

@author: WKOUBAA
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime
import time
import json
from dateutil.relativedelta import relativedelta
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
from streamlit_option_menu import option_menu
import os
import utils
import plotly.express as px
from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload 
from google.oauth2 import service_account 
from io import BytesIO, StringIO



# =============================================================================
# Google Drive API functions
# =============================================================================
def create_folder(api_service,Folder_name,Folder_id):
    

    try:
        # create drive api client
        if Folder_id is None:
            file_metadata = {
                'name': Folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': []
            }
        else:
            file_metadata = {
                'name': Folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [Folder_id]
            }
        # pylint: disable=maybe-no-member
        file = api_service.files().create(body=file_metadata, fields='id'
                                      ).execute()
        print(F'Folder ID: "{file.get("id")}".')
        return file.get('id')

    except HttpError as error:
        print(F'An error occurred: {error}')
        return None

def search_file(api_service,Folder_name,Folder_id,Type):
    

    try:

        files = []
        folder_found=False
        folder_id=None
        page_token = None
        while True:
            # pylint: disable=maybe-no-member
            if Folder_id is None:
                query="mimeType='"+Type+"' and 'root' in parents"
            else: 
                query="mimeType='"+Type+"' and parents in '{}'".format(Folder_id)

            response = api_service.files().list(q=query,
                                            spaces='drive',
                                            fields='nextPageToken, '
                                                   'files(id, name)',
                                            pageToken=page_token).execute()
                    
            #for file in response.get('files', []):
                # Process change
             #   print(F'Found file: {file.get("name")}, {file.get("id")}')
            
            files.extend(response.get('files', []))
            for iFolder in files:
                if Folder_name==iFolder['name']:
                    folder_found=True
                    folder_id=iFolder['id']
                    break
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    except HttpError as error:
        print(F'An error occurred: {error}')
        folder_found = None

    return [folder_found,folder_id]

def Download_File(api_service,File_id):
    file=[]
    FStatus=False
    try:
        request = api_service.files().get_media(fileId=File_id)
        file = BytesIO()
        downloader = MediaIoBaseDownload(file,request)
        done = False
        while done is False: _,done = downloader.next_chunk()
        file.seek(0)
        FStatus=True
    except HttpError as error:
        print(F'An error occurred: {error}')
        
    return [FStatus,file]

def Upload_DataFrame(api_service,df,Folder_id,File_name):
    FStatus=False
    try:
        stream = BytesIO()
        # writing df to the stream instead of a file:
        df.to_csv(stream, sep=',', encoding='utf-8-sig', index = False)
        media = MediaIoBaseUpload(stream,
                              mimetype='application/octet-stream',
                              resumable=True)
        file_metadata = {
            'name': File_name,
            'parents': [Folder_id]
        }   
    
        #media_content = MediaFileUpload('test.csv', mimetype='text/csv')
        file = api_service.files().create(
            body=file_metadata,
            media_body=media
        ).execute()
        
    except HttpError as error:
        print(F'An error occurred: {error}')
    return FStatus

def delete_file(api_service, file_id):

    try:
      api_service.files().delete(fileId=file_id).execute()
        
    except HttpError as error:
        print(F'An error occurred: {error}')
# =============================================================================
#     end
# =============================================================================
#@st.cache(allow_output_mutation=True)
def load_data(drive,folder_id):
    Type_Folder='application/vnd.google-apps.folder'
    Type_csv='text/csv'
    #[folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
    
    [DataBase_exist,DataBase_id]=search_file(drive,'DataBase',folder_id,Type_Folder)
    [File_exist,File_id]=search_file(drive,'WixData.csv',DataBase_id,Type_csv)
    [FStatus,file]=Download_File(drive,File_id)
    WixData=pd.read_csv(file)
    #WixData=pd.read_csv('.\DataBase\WixData.csv')
    return WixData

def load_reservation(drive,folder_id):
    Type_Folder='application/vnd.google-apps.folder'
    Type_csv='text/csv'
    #[folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
    
    [DataBase_exist,DataBase_id]=search_file(drive,'DataBase',folder_id,Type_Folder)
    [File_exist,File_id]=search_file(drive,'liste_des_réservations.csv',DataBase_id,Type_csv)
    if File_exist:
        [FStatus,file]=Download_File(drive,File_id)
        Reservation=pd.read_csv(file)
        Reservation['Numéro de téléphone du client']=Reservation['Numéro de téléphone du client'].apply(lambda x:process_number(x,0))

    else:
        Reservation=[]
    return Reservation

#
#@st.cache()
@st.cache(allow_output_mutation=True)
def load_data_Small():
    
    return [st.session_state.WixData,st.session_state.Reservation]

#@st.cache(allow_output_mutation=True)
def load_summary(drive,folder_id):
    # [folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
    #folder_id=st.secrets['Orange_SMS_ID']['folderID']
    [History_exist,History_id]=search_file(drive,'History_Compaign',folder_id,Type_Folder)
    [File_exist,File_id]=search_file(drive,'Summary_compaign.csv',History_id,Type_csv)
    if File_exist:
        [FStatus,file]=Download_File(drive,File_id)
        Summary=pd.read_csv(file)
    else:
        Summary=pd.DataFrame()
    #Summary=pd.read_csv('.\History_Compaign\Summary_compaign.csv')
    return Summary

def load_liste(drive,folder_id):
    Type_Folder='application/vnd.google-apps.folder'
    Type_csv='text/csv'
    liste=pd.DataFrame()
    #[folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
    
    [DataBase_exist,DataBase_id]=search_file(drive,'DataBase',folder_id,Type_Folder)
    [File_exist,File_id]=search_file(drive,'Liste_a_appeler.csv',DataBase_id,Type_csv)
    
    if File_exist:
        [FStatus,file]=Download_File(drive,File_id)
        liste=pd.read_csv(file)
    #WixData=pd.read_csv('.\DataBase\WixData.csv')
    return liste
@st.cache
def liste_label(WixData):
    label=WixData.Labels[WixData.Labels.notnull()].unique()
    liste_label=[]
    for i in range(0,len(label)):
       
        labels_i=label[i].split(";")
        for j in labels_i:
            if j not in liste_label:
                liste_label.append(j)
    return np.array(liste_label)


def get_status_sms(AUTH_TOKEN, SENDER_NAME):
    sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
    Balance=sms.showBalanceSMS()
    return Balance

@st.cache
def select_rows_label(df,label_i):
    return df.index[df['Labels'].notnull() & df['Labels'].str.contains(label_i)].to_list()

@st.cache
def unique(list1):
 
    # initialize a null list
    unique_list = []
 
    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
    return unique_list
@st.cache
def find_client_reserved(WixData,Reservation):
    Client=WixData[['First Name','Last Name']].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    tel1=WixData['Phone 1'].apply(str)
    tel1=tel1.apply(lambda x: x.replace(' ',''))
    telres=Reservation['Numéro de téléphone du client'].apply(str)
    indx_Wix=list()
    Client_NotFound=list()
    for icus in range(0,len(Reservation['Nom du client'])):
        if Reservation['Nom du service'][icus]=='Entretien osmoseur domestique':
            name_i=Reservation['Nom du client'][icus]
            name_list=name_i.split(' ')
            name_u=[]
            for iname in name_list:
                if iname not in name_u:
                    name_u.append(iname)
            name_i=' '.join(name_u)
            indx=Client.str.contains(name_i)
            indx=indx[indx==True]
            if len(indx.index.tolist())==0:
                teli=telres[icus].replace(' ','')
                indx1=tel1[tel1==teli]
                if len(indx1.index.tolist())==0:
                    if ('-' in teli):
                        teli=teli.split('-')
                    elif ('*' in teli):
                        teli=teli.split('*')
                    elif  ('/' in teli):
                        teli=teli.split('/')
                    else:
                        teli=[teli]
                    for itel in range(0,len(teli)):
                        telii=teli[itel].replace(' ','')
                        telii=telii[-8:]
                        indx1=tel1.str.contains(telii)
                        indx1=indx1[indx1==True]
                        if len(indx1.index.tolist())>0:
                            break
                if len(indx1.index.tolist())!=0:
                    indx_Wix.append(indx1.index.tolist()[0])
                else:
                    Client_NotFound.append(Reservation['Nom du client'][icus])
                    
                    
    
    
            else:
                indx_Wix.append(indx.index.tolist()[0])
    
    return [indx_Wix,Client_NotFound]

@st.cache
def select_rows_entretien(df,mois,indx_Wix):
    dayE = date.today()+ relativedelta(months=-mois)
    dayE =dayE.strftime('%Y-%m-%d')
    #Date entretien n'hexiste pas / voyant date installation
    condition1=df.index[df['Dernier entretien'].isnull() & df["Date d'installation"].notnull() & (df['Labels'].str.contains("Pas interesse par nos services! ne plus contacter")==False)].to_list()
    condition2=df.index[df['Dernier entretien'].isnull() & df["Date d'installation"].notnull() & (df['Labels'].str.contains("Client ne repond pas a nos appelles! ne plus contacter")==False)].to_list()
    condition2=condition1+condition2
    condition2=unique(condition2)
    condition1=df.index[df["Date d'installation"]<=dayE].to_list()
    condition1=list(set(condition1) & set(condition2))
    #Date entretien existe
    condition2=df.index[df['Dernier entretien']<=dayE].to_list()
    condition3=df.index[df['Dernier entretien'].isnull() & df["Date d'installation"].isnull()].tolist()
    condition4=df.index[(df['Labels'].str.contains("Client ne repond pas a nos appelles! ne plus contacter")==True)].tolist() 
    condition5=df.index[(df['Labels'].str.contains("Pas interesse par nos services! ne plus contacter")==True)].tolist() 
    condition6=df.index[df['Labels'].str.contains("Installation") ==True ].tolist()
    condition7=df.index[df['Labels'].str.contains("Entretien")==True].tolist()
    condition8=df.index[df['Labels'].str.contains("Réparation osmoseur domestique")==True].tolist()
    condition3=list(set(condition3)  & set(condition5) & set(condition6) & set(condition7) & set(condition8))
    condition=unique(condition1+condition2+condition3)
    condition=list(set(condition) - set(condition4) -set(condition5))
    condition=list(set(condition)-set(indx_Wix))
    return condition
def create_list(SelectedData,Ancienne_Liste):
    if 'First Name' in list(Ancienne_Liste.columns):
        Client=list(Ancienne_Liste[['First Name','Last Name']].apply(lambda row: ' '.join(row.values.astype(str)).replace(' ','').lower(), axis=1))
        SelectedData=SelectedData.sort_values(by = 'Dernier entretien')
        SelectedData['Client']=SelectedData[['First Name','Last Name']].apply(lambda row: ' '.join(row.values.astype(str)).replace(' ','').lower(), axis=1)
        SelectedData['Exist']=SelectedData['Client'].apply(lambda x: x in Client)
        SelectedData=SelectedData[SelectedData['Exist']==False]
    else:
        SelectedData=SelectedData.sort_values(by = 'Dernier entretien')
        SelectedData['Client']=SelectedData[['First Name','Last Name']].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        
    if not(SelectedData.empty):
        N_old=30
        N_new=20
        if len(SelectedData['Client'])>=50:
            Liste_old=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']][0:N_old]
            Liste_new=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']][-N_new:]
            Liste=pd.concat([Liste_old,Liste_new])
        elif len(SelectedData['Client'])>30:
            N_new=len(SelectedData['Client'])-N_old
            Liste_old=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']][0:N_old]
            Liste_new=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']][-N_new:]
            Liste=pd.concat([Liste_old,Liste_new])    
        else:
            Liste=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']]
    else:
        Liste=SelectedData[['Client','First Name','Last Name',"Date d'installation",'Dernier entretien']]
    return Liste


        
        
        
        
    
    

def process_number(Number,indx):
    if pd.isnull(Number):
        Number=''
    Number=str(Number)
    # split 
    Special_charac=['/','*','-']
    Tel=[]
    for i in Special_charac:
        if i in Number:
            Tel=Number.split(i)
            break
    if len(Tel)!=0:
        Number=Tel[indx]
    elif indx>0:
        Number=''
        
        
    Number=Number.replace(' ','')
    Number=Number.replace('.','')
    Number=Number.replace(',','')
    if len(Number)<8:
        Number=''
    elif len(Number)>8:

        Number=Number[-8:]
        Number='216'+Number
    else:
        Number='216'+Number
    if Number=='' or not(Number.isnumeric()):
        Number=np.nan
    return Number    
@st.cache 
def process_message(Message):
    Indx1=Message.find("{{")
    Indx2=Message.find("}}")
    field=''
    if Indx1!=-1:
        field=Message[Indx1+2:Indx2]
    return [Indx1,Indx2,field]

@st.cache
def find_dupicated(df):
    res=df.reset_index().groupby(df['Phone 1'].tolist())["index"].agg(list).reset_index().rename(columns={"index": "duplicated"})
    res.index=res["duplicated"].str[0].tolist()
    res["duplicated"]=res["duplicated"].str[1:]
    res['duplicated']=res['duplicated'].apply(lambda x: np.nan if len(x)==0 else x)
    ind=res.index[pd.notnull(res['duplicated'])].tolist()
    Final_index=[]
    for i in range(0,len(ind)):
        indx_i=ind[i]
        indx_to_keep=indx_i
        lignep=df.iloc[indx_to_keep]
        lignep=lignep[lignep.notnull()].count()
        for j in res['duplicated'][indx_i]:
            indx_d=j
            lignej=df.iloc[indx_d]
            lignej=lignej[lignej.notnull()].count()
            if lignej>lignep:
                indx_to_keep=indx_d
                lignep=df.iloc[indx_to_keep]
                lignep=lignep[lignep.notnull()].count() 
        
        Final_index.append(indx_to_keep)
    ind=res.index[pd.isnull(res['duplicated'])].tolist()
    Final_index=Final_index+ind
    Final_index.sort()
        
    return Final_index

@st.cache
def Get_Phone_Summary(df):
    prefix=('2167','2163','nan')
    tel1=df['Phone 1'][df['Phone 1'].notnull()]
    tel1=tel1.astype(str)
    tel1=tel1.apply(lambda x: x[0:11] if pd.notnull(x) else x)
    
    indx1=tel1.str.startswith(prefix,na=True)
    Nb1=len(tel1[indx1==False])
    tel2=df['Phone 2'][df['Phone 2'].notnull()]
    tel2=tel2.apply(str)
    tel2=tel2.apply(lambda x: x[0:11] if pd.notnull(x) else x)
    indx2=tel2.str.startswith(prefix,na=True)
    Nb2=len(tel2[indx2==False])
    indx_rep=tel2.apply(lambda x: tel1.index[tel1==x].tolist())
    indx_rep_NE=indx_rep.apply(lambda x: True if len(x)!=0 else False)
    indx_rep=indx_rep.index[indx_rep_NE==True].tolist()
    tel2_to_ignore=list(tel2[indx_rep])
    return [Nb1,Nb2-len(tel2_to_ignore),tel2_to_ignore]
    

def post_process_DataBase(WixData):
        WixDataSmall=WixData[['First Name',"Last Name","Phone 1","Phone 2","Labels","Date d'installation","Dernier entretien"]][WixData["Phone 1"].notnull() | WixData["Phone 2"].notnull()]
        WixDataSmall=WixDataSmall.reset_index(drop=True)
        WixDataSmall["Phone 1_P"]=WixDataSmall["Phone 1"].apply(lambda x: process_number(x,0))
        WixDataSmall["Phone 2_P"]= [process_number(x,1) if pd.notnull(process_number(x,1)) & pd.isnull(y)  else process_number(y,0)  for x,y in zip(WixDataSmall['Phone 1'],WixDataSmall['Phone 2'])]
        WixDataSmall["Phone 2_P"] = [process_number(x,1) if pd.notnull(t) & (z==t) & (process_number(x,1)!=process_number(y,0)) else pd.nan if pd.notnull(t) & (z==t) else t  for x,y,z,t in zip(WixDataSmall['Phone 1'],WixDataSmall['Phone 2'],WixDataSmall['Phone 1_P'],WixDataSmall['Phone 2_P'])]
        WixDataSmall["Phone 1"] = WixDataSmall["Phone 1_P"]
        WixDataSmall["Phone 2"] = WixDataSmall["Phone 2_P"]
        WixDataSmall = WixDataSmall.drop(['Phone 1_P','Phone 2_P'], axis=1)
        WixDataSmall=WixDataSmall[:][WixDataSmall["Phone 1"].notnull() | WixDataSmall["Phone 2"].notnull()]
        WixDataSmall=WixDataSmall.reset_index(drop=True)
        indx=find_dupicated(WixDataSmall)
        WixDataSmall=WixDataSmall.iloc[indx]
        WixDataSmall=WixDataSmall.reset_index(drop=True)
        WixDataSmall["Phone 1"]=WixDataSmall["Phone 1"].apply(str)
        WixDataSmall["Phone 2"]=WixDataSmall["Phone 2"].apply(str)
          
        return WixDataSmall

def processing_message_i(Message,WixData):
    [Indx1,Indx2,field]=process_message(Message)


def process_credential(Cred):
    Cred['refresh_token']=None
    Cred['user_agent']=None
    Cred['id_token']=None
    Cred['id_token_jwt']=None
    return Cred

def check_password():
    """Returns `True` if the user had a correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if (
            st.session_state["username"] in st.secrets["passwords"]
            and st.session_state["password"]
            == st.secrets["passwords"][st.session_state["username"]]
        ):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store username + password
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show inputs for username + password.
        st.text_input("Username", on_change=password_entered, key="username")
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input("Username", on_change=password_entered, key="username")
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 User not known or password incorrect")
        return False
    else:
        # Password correct.
        return True


# define path variables
if check_password():
# =============================================================================
#     Orange-SMS-Tunisia API
# =============================================================================
    SENDER_NAME = 'Aqua World'
    AUTH_TOKEN =st.secrets["AUTH_TOKEN"]
    Type_Folder='application/vnd.google-apps.folder'
    Type_csv='text/csv'
    # =============================================================================
    #         # Use google drive Data base:
    # =============================================================================    
    
    # define API scope
    SCOPE = 'https://www.googleapis.com/auth/drive'
    credentials = service_account.Credentials.from_service_account_info(st.secrets['GOOGLE_DRIVE_TOKEN'],scopes=[SCOPE])
    drive = discovery.build('drive', 'v3', credentials=credentials)
    folder_id=st.secrets['Orange_SMS_ID']['folderID']
    # =============================================================================
    # Dashbord 
    # =============================================================================
    
    
    with st.sidebar:
        selected = option_menu("Menu Principal", ["Nouvelle compagne", "Compagnes"], 
            icons=["envelope-open", "file-earmark-ruled"], menu_icon="filter-square", default_index=0)
    
    if selected=="Nouvelle compagne":
        #sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
        Balance=get_status_sms(AUTH_TOKEN, SENDER_NAME)
        st.title('_Détails de la compagne SMS_')
        st.header("_Information regardant l'abonnement_")
        col1, col2, col3 = st.columns(3)
        col1.metric("Date d'expiration",Balance[0]['expirationDate'][0:10] , None)
        col2.metric("Status",Balance[0]['status'] , None)
        col3.metric("Messages Restant",Balance[0]['availableUnits'] , None)
        col1, col2, col3 = st.columns(3)
        with col1:
            title = st.text_input('Nom de la compagne SMS', 'Entrer, ici le nom')
            if title=='Entrer, ici le nom':
                title=''
        
            
        txt = st.text_area("Message SMS","Entrer, ici votre message")
        if txt=="Entrer, ici votre message":
            txt=''
        selected2 = option_menu(None, ["Sélection à partir des contacts", "Entrée manuelle des numéros"], 
        icons=['person-lines-fill', 'journal-arrow-up'], 
        menu_icon="cast", default_index=0, orientation="horizontal")
        if selected2=="Sélection à partir des contacts":
    
            
    # =============================================================================
    #         #Preparing Data
    # =============================================================================
            if "Loaded" not in st.session_state:
                
                st.session_state.Loaded = False
                st.session_state.WixData =[]
                st.session_state.Reservation =[]
            
            if st.session_state.Loaded:
                
                #WixDataSmall=load_data_Small(drive,folder_id)
                [WixDataSmall,Reservation]=load_data_Small()
                Wixlabel= liste_label(WixDataSmall)
                st.session_state.Loaded=True
            
            else:
                WixData=load_data(drive,folder_id)
                WixDataSmall=post_process_DataBase(WixData)
                # [folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
                [DataBase_exist,DataBase_id]=search_file(drive,'DataBase',folder_id,Type_Folder)
                [File_exist,File_id]=search_file(drive,'WixDataSmall.csv',DataBase_id,Type_csv)
                if File_exist:
                    delete_file(drive, File_id)
                FStatus=Upload_DataFrame(drive,WixDataSmall,DataBase_id,'WixDataSmall.csv')
                st.session_state.WixData=WixDataSmall
                Wixlabel= liste_label(WixDataSmall)
                Reservation=load_reservation(drive,folder_id)
    
                st.session_state.Reservation=Reservation
                st.session_state.Loaded=True
            # 
            if "disabled" not in st.session_state:
                
                st.session_state.disabled = False
                indx_selection=[]
    
            if "disabled_E" not in st.session_state:
                
                st.session_state.disabled_E = False
                indx_selection=[]
    
            
            if "Selection" not in st.session_state:
                st.session_state.Selection = "Sélection Manuelle"
                st.session_state.disabled = False
                st.session_state.disabled_E = False
    
    
            col1, col2 = st.columns(2)
            with col1:
                #st.checkbox("Sélection par label", key="disabled")
                st.radio(
                    "Type de Sélection",
                    ["Sélection Manuelle","Sélection par label", "Sélection pour entretien"],
                    key="Selection",
                    
                )
       
            if st.session_state.Selection !="Sélection par label":
                st.session_state.disabled=False
            else:
                st.session_state.disabled=True
            
            if st.session_state.Selection !="Sélection pour entretien":
                st.session_state.disabled_E=False
            else:
                st.session_state.disabled_E=True        
    
           
            with col2:
                option = st.selectbox(
                    "Choisissez le label",
                    Wixlabel,
                    
                    disabled=not(st.session_state.disabled),
                )
                option2 = st.selectbox(
                    "Choisissez la période depuis la dernière intervention",
                    (">= 2 mois",">= 3 mois",">= 4 mois",">= 5 mois",">= 6 mois",">= 12 mois"),
                    index=4,
                    disabled=not(st.session_state.disabled_E),
                )        
            if  st.session_state.disabled:
                indx_selection=select_rows_label(WixDataSmall,option)
            elif st.session_state.disabled_E:
                indx_Wix=[];
                if isinstance(Reservation, pd.DataFrame):
                    [indx_Wix,Client_NotFound]=find_client_reserved(WixDataSmall,Reservation)
                if len(indx_Wix)>0:
                    col1.write('Il y a deja : '+str(len(indx_Wix))+' entretiens programés')
                indx_selection=select_rows_entretien(WixDataSmall,int(option2[3:5]),indx_Wix)
            else:
                indx_selection=[]
            
            Nb_selection=len(indx_selection)
            col1.metric("Nombre de contacts pré-sélectionnés",Nb_selection , None)
            st.header("Appuyer sur _UPDATE_ apres selection")
            #Initialzation of Aggrid
            gb = GridOptionsBuilder.from_dataframe(WixDataSmall)
            gb.configure_column("First Name", headerCheckboxSelection = True)
            gb.configure_selection(selection_mode="multiple", use_checkbox=True,pre_selected_rows=indx_selection)
            gridOptions = gb.build()
            response = AgGrid(
            WixDataSmall,
            gridOptions=gridOptions,
            reload_data=False,
            enable_enterprise_modules=False,
            width='100%',
            height=300,
            update_mode=GridUpdateMode.MANUAL,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True
            )
            Nb_selection=len(response["selected_rows"])
            
            #Continue Here
            SelectedData=pd.DataFrame(response["selected_rows"])
            tel1=0
            tel2=0
            tel2_i=[]
            #st.write(response["selected_rows"])
            if Nb_selection>0:
                [tel1,tel2,tel2_i]=Get_Phone_Summary(SelectedData)
                
            st.header("Sommaire Compagne:: _"+title+"_ :")
            if "disable_B" not in st.session_state:
                st.session_state.disable_B = False
            message="Faites un Update la table ☝"
            if tel1+tel2>0 and tel1+tel2<=int(Balance[0]['availableUnits']):
                message="Compagne prête a l'envoi 👍"
                st.session_state.disable_B = True
            elif tel1+tel2> int(Balance[0]['availableUnits']):
                message="Contacts sélectionnés sont supérieurs aux SMS restants 👎"
                st.session_state.disable_B = False
                
                    
            col1, col2, col3 = st.columns(3)
            
               
            col2.metric("Contacts sélectionnés:",Nb_selection , None)
            col3.metric("Nombre SMS Total:",tel1+tel2 , None)
            col2.metric("Nb SMS Phone 1:",tel1 , None)
            col3.metric("Nb SMS Phone 2:",tel2 , None)
            B=False
            T=False
            with col1:
                st.write(message)
    
                B=st.button('Envoyer la compagne',disabled=not(st.session_state.disable_B))
                if st.session_state.Selection =="Sélection pour entretien":
                    T=st.button("Gener liste d'appel pour entretien")
                    if T:#upload ancienne liste
                        Ancienne_Liste=load_liste(drive,folder_id)
                        Liste_a_appeler=create_list(SelectedData,Ancienne_Liste)
                        [DataBase_exist,DataBase_id]=search_file(drive,'DataBase',folder_id,Type_Folder)
                        [File_exist,File_id]=search_file(drive,'Liste_a_appeler.csv',DataBase_id,Type_csv)
                        
                        if File_exist:
                            delete_file(drive, File_id)
                        FStatus=Upload_DataFrame(drive,Liste_a_appeler,DataBase_id,'Liste_a_appeler.csv')
                        
                    
            if B:
                # Post processing messages
                StartCompagne=datetime.now().strftime('Date %Y-%m-%d Heure %H-%M-%S')
                recipient_phone_number='21698511119'
                dev_phone_number='21698511119'
                sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
                del(SelectedData["_selectedRowNodeInfo"])
                SelectedData.insert(3,'Phone1 Status',SelectedData["Phone 1"])
                SelectedData.insert(5,'Phone2 Status',SelectedData["Phone 2"])
                SelectedData['Phone 1']=SelectedData['Phone 1'].apply(str)
                SelectedData['Phone 2']=SelectedData['Phone 2'].apply(str)
                SelectedData['Envoyee']=SelectedData['Phone 2'].apply(lambda x: 'OK')
                col1.write("C'est parti ✉...📬")
                [Indx1,Indx2,field]=process_message(txt)
                list_column=list(SelectedData.columns)
                if not(field in list_column):
                    field=''
                my_bar = st.progress(0)
                count_SMS=0
                count_SMS_Success=0
                prefix=('2167','2163','nan')
                
                for icus in range(0,len(SelectedData['Phone 1'])):
                    try:
                        if field !='':
                            Message_i=txt.replace(txt[Indx1:Indx2+2],SelectedData[field][icus])
                        else:
                            Message_i=txt.replace(txt[Indx1:Indx2+2],field)
                        
                        Phone_i=SelectedData['Phone 1'][icus]
                        Phone_i=Phone_i[0:11]
                        if not(Phone_i.startswith(prefix)) and Phone_i!='nan':
                            res = sms.send_sms(message=Message_i,
                                                dev_phone_number=dev_phone_number,
                                                recipient_phone_number=Phone_i)
        
                            if res.status_code == 201:
                                count_SMS_Success=count_SMS_Success+1
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Reussi, '+now
                            else:
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Echec, '+now
                            SelectedData['Phone1 Status'][icus]=status
                            count_SMS_Success=count_SMS_Success+1
                            count_SMS=count_SMS+1
                            time.sleep(0.5)
                            my_bar.progress(min(count_SMS/(tel1+tel2),1.0))
                        elif Phone_i.startswith(prefix):
                            now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                            status='Fixe non envoye, '+now
                            SelectedData['Phone1 Status'][icus]=status
                            
                        
                        Phone_i=SelectedData['Phone 2'][icus]
                        Phone_i=Phone_i[0:11]
                        
                        if not(Phone_i.startswith(prefix)) and Phone_i!='nan':
                            res = sms.send_sms(message=Message_i,
                                                dev_phone_number=dev_phone_number,
                                                recipient_phone_number=Phone_i)
        
                            if res.status_code == 201:
                                count_SMS_Success=count_SMS_Success+1
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Reussi, '+now
                            else:
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Echec, '+now
                            SelectedData['Phone 2'][icus]=Phone_i
                            SelectedData['Phone2 Status'][icus]=status
                            count_SMS_Success=count_SMS_Success+1
                            count_SMS=count_SMS+1
                            time.sleep(0.5)
                            my_bar.progress(min(count_SMS/(tel1+tel2),1.0))
                        elif Phone_i.startswith(prefix):
                            SelectedData['Phone 2'][icus]=Phone_i
                            now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                            status='Fixe non envoye, '+now
                            SelectedData['Phone2 Status'][icus]=status
                    except:
                        errori="erreur"
                        try:
                            sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
                        except:
                            errori="connection error"
                        SelectedData['Envoyee'][icus]=errori
                        
                
                Balance=get_status_sms(AUTH_TOKEN, SENDER_NAME)
                SelectedData['Phone 1']=SelectedData['Phone 1'].apply(str)
                SelectedData['Phone 2']=SelectedData['Phone 2'].apply(str)
                
                File_name=title+' - '+StartCompagne+'.csv'
                
                # [folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
                folder_id=st.secrets['Orange_SMS_ID']['folderID']
                [History_exist,History_id]=search_file(drive,'History_Compaign',folder_id,Type_Folder)
                [File_exist,File_id]=search_file(drive,File_name,History_id,Type_csv)
                if File_exist:
                    delete_file(drive, File_id)
                    
                FStatus=Upload_DataFrame(drive,SelectedData,History_id,File_name)
                Summary_Compagne=[{'Nom compagne':title,'Date creation':StartCompagne,"Taux de reussite":100*count_SMS_Success/(tel1+tel2),'Message':txt,'Nombre Contacts':Nb_selection,'SMS Envoye Tel1':tel1,'SMS Envoye Tel2':tel2}]
                #st.write(Summary_Compagne)
                Summary_Compagne=pd.DataFrame(Summary_Compagne)
                
                File_name='Summary_compaign.csv'
                [File_exist,File_id]=search_file(drive,File_name,History_id,Type_csv)
                if File_exist:
                    [FStatus,file]=Download_File(drive,File_id)
                    Summary_old=pd.read_csv(file)
                    Summary_Compagne=pd.concat([Summary_old, Summary_Compagne], axis=0)
                    delete_file(drive, File_id)
                    FStatus=Upload_DataFrame(drive,Summary_Compagne,History_id,File_name)
                else:
                    FStatus=Upload_DataFrame(drive,Summary_Compagne,History_id,File_name)
                
                pie_chart = px.pie(SelectedData,
                       title="Taux de réussite de la compagne SMS "+ title,
                       values=[100*count_SMS_Success/(tel1+tel2),100-100*count_SMS_Success/(tel1+tel2)],
                       names=["Réussite","Echec"])
                st.plotly_chart(pie_chart)
                    
                    
            ## Selection a partir de donnees manuelle
        else:
            if "type_entree" not in st.session_state:
                st.session_state.type_entree = False
            col1, col2 = st.columns(2)
            with col1:
                 st.checkbox("A partir d'un fichier csv",key="type_entree")
            with col2:
                uploaded_file = st.file_uploader("Chosissez un document",disabled=not(st.session_state.type_entree))
                df1=pd.DataFrame()
                if uploaded_file is not None:
                    df1 = pd.read_csv(uploaded_file,header=None)
                    if len(df1.columns)>1:
                        df1=df1.rename(columns={0: "Name"})
                        df1=df1.rename(columns={1: "Phone 1"})
                    
            Phones_Manuel = st.text_area("Liste des telephones:","Entrer la liste des numeros separes par un ','",disabled=st.session_state.type_entree)
            if Phones_Manuel=="Entrer la liste des numeros separes par un ','":
                df2=pd.DataFrame()
            else:
                list_phone=Phones_Manuel.split(',')
                df2=pd.DataFrame(list_phone)
                df2=df2.rename(columns={0: "Phone 1"})
            df=df2
            if st.session_state.type_entree:
                df=df1
            tel1=0
            if not(df.empty):
                prefix=('2167','2163')
                st.write(df)
                df["Phone 1"]=df["Phone 1"].apply(lambda x: process_number(x,0))
                st.write(df)
                df=pd.DataFrame(df["Phone 1"].unique())
                df=df.rename(columns={0: "Phone 1"})
                indx=df["Phone 1"].str.startswith(prefix,na=True)
                df=pd.DataFrame(df["Phone 1"][indx==False])
                df=df.rename(columns={0: "Phone 1"})
                
                tel1=len(df["Phone 1"])
            st.header("Sommaire Compagne:: _"+title+"_ :")
            
            
            if "disable_B2" not in st.session_state:
                st.session_state.disable_B2 = False
            if tel1>0 and tel1<=int(Balance[0]['availableUnits']):
                message="Compagne prête a l'envoi 👍"
                st.session_state.disable_B2 = True
            elif tel1==0:
                message="Pas de numero valide dans la liste ci-dessus ☝"
                st.session_state.disable_B2 = False
            elif tel1> int(Balance[0]['availableUnits']):
                message="Contacts sélectionnés sont supérieurs aux SMS restants 👎"
                st.session_state.disable_B2 = False
            col1, col2 = st.columns(2)
            
            col2.metric("Nombre SMS Total:",tel1 , None)
            B=False
            with col1:
                st.write(message)
    
                B=st.button('Envoyer la compagne',disabled=not(st.session_state.disable_B2))
            if B:
                StartCompagne=datetime.now().strftime('Date %Y-%m-%d Heure %H-%M-%S')
                recipient_phone_number='21698511119'
                dev_phone_number='21698511119'
                sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
                
                col1.write("C'est parti ✉...📬")
                df.insert(1,'Phone1 Status',df["Phone 1"])
                st.write(df)
                df['Envoyee']=df['Phone1 Status'].apply(lambda x: 'OK')
                [Indx1,Indx2,field]=process_message(txt)
                list_column=list(df.columns)    
                if not(field in list_column):
                    field=''
                my_bar = st.progress(0)
                count_SMS=0
                count_SMS_Success=0
                prefix=('2167','2163')
                for icus in range(0,len(df['Phone 1'])):
                    try:
                        if field !='':
                            Message_i=txt.replace(txt[Indx1:Indx2+2],df[field][icus])
                        else:
                            Message_i=txt.replace(txt[Indx1:Indx2+2],field)
                        
                        Phone_i=df['Phone 1'][icus]
                        Phone_i=Phone_i[0:11]
                        if not(Phone_i.startswith(prefix)) and Phone_i!='nan':
                            res = sms.send_sms(message=Message_i,
                                                dev_phone_number=dev_phone_number,
                                                recipient_phone_number=Phone_i)
        
                            if res.status_code == 201:
                                count_SMS_Success=count_SMS_Success+1
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Reussi, '+now
                            else:
                                now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                                status='Echec, '+now
                                
                            df['Phone1 Status'][icus]=status
                            # count_SMS_Success=count_SMS_Success+1
                            count_SMS=count_SMS+1
                            time.sleep(0.5)
                            my_bar.progress(min(count_SMS/(tel1),1.0))
                        elif Phone_i.startswith(prefix):
                            now=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                            status='Fixe non envoye, '+now
                            df['Phone1 Status'][icus]=status
                    except:
                        errori="erreur"
                        try:
                            sms = utils.SMS( AUTH_TOKEN, SENDER_NAME)
                        except:
                            errori="connection error"
                        df['Envoyee'][icus]=errori
                
                Balance=get_status_sms(AUTH_TOKEN, SENDER_NAME)
                df['Phone 1']=df['Phone 1'].apply(str)
                File_name=title+' - '+StartCompagne+'.csv'
                
                # [folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
                folder_id=st.secrets['Orange_SMS_ID']['folderID']
                [History_exist,History_id]=search_file(drive,'History_Compaign',folder_id,Type_Folder)
                [File_exist,File_id]=search_file(drive,File_name,History_id,Type_csv)
                if File_exist:
                    delete_file(drive, File_id)
                    
                FStatus=Upload_DataFrame(drive,df,History_id,File_name)
                Summary_Compagne=[{'Nom compagne':title,'Date creation':StartCompagne,"Taux de reussite":100*count_SMS_Success/(tel1),'Message':txt,'Nombre Contacts':tel1,'SMS Envoye Tel1':tel1,'SMS Envoye Tel2':0}]
                #st.write(Summary_Compagne)
                Summary_Compagne=pd.DataFrame(Summary_Compagne)
                
                File_name='Summary_compaign.csv'
                [File_exist,File_id]=search_file(drive,File_name,History_id,Type_csv)
                if File_exist:
                    [FStatus,file]=Download_File(drive,File_id)
                    Summary_old=pd.read_csv(file)
                    Summary_Compagne=pd.concat([Summary_old, Summary_Compagne], axis=0)
                    delete_file(drive, File_id)
                    FStatus=Upload_DataFrame(drive,Summary_Compagne,History_id,File_name)
                else:
                    FStatus=Upload_DataFrame(drive,Summary_Compagne,History_id,File_name)
                
                pie_chart = px.pie(df,
                       title="Taux de réussite de la compagne SMS "+ title,
                       values=[100*count_SMS_Success/(tel1),100-100*count_SMS_Success/(tel1)],
                       names=["Réussite","Echec"])
                st.plotly_chart(pie_chart)
                
                
    if selected=="Compagnes":
        st.title('_Historique des compagnes SMS_')
        st.header("_Liste des compagnes envoyées_")
        Summary_Compagne=load_summary(drive,folder_id)
        gb = GridOptionsBuilder.from_dataframe(Summary_Compagne)
        gb.configure_column("Nom compagne", headerCheckboxSelection =False)
        gb.configure_selection(selection_mode="single", use_checkbox=True)
        gridOptions = gb.build()
        response = AgGrid(
        Summary_Compagne,
        gridOptions=gridOptions,
        reload_data=False,
        enable_enterprise_modules=False,
        width='100%',
        height=200,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        fit_columns_on_grid_load=True
        )
        Nb_selection=len(response["selected_rows"])
        if "disable_B3" not in st.session_state:
            st.session_state.disable_B3 = False
        if Nb_selection==0:
            st.session_state.disable_B3 = False
        else:
            st.session_state.disable_B3 = True
        B=False    
        B=st.button('Afficher les details de la compagne',disabled=not(st.session_state.disable_B3))
        if B:
            if pd.isnull(response["selected_rows"][0]["Nom compagne"]):
                File_name=' - '+response["selected_rows"][0]["Date creation"]+'.csv'
            else:
                File_name=response["selected_rows"][0]["Nom compagne"] +' - '+response["selected_rows"][0]["Date creation"]+'.csv'
    
                
            # [folder_exist,folder_id]=search_file(drive,'Orange-SMS',None,Type_Folder)
            folder_id=st.secrets['Orange_SMS_ID']['folderID']
            [History_exist,History_id]=search_file(drive,'History_Compaign',folder_id,Type_Folder)
            [File_exist,File_id]=search_file(drive,File_name,History_id,Type_csv)
            if File_exist:
                 [FStatus,file]=Download_File(drive,File_id)
                 detail=pd.read_csv(file)
            else:
                 detail=pd.DataFrame()
            st.dataframe(detail)
        
        
        
        
        

        
                
                
                
                
        
    
#selected2