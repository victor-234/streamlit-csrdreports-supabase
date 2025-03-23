import os
import io
import time
import dotenv
import pymupdf
import tusclient

import pandas as pd
import streamlit as st

from mistralai import Mistral
from supabase import create_client, Client
from tusclient.exceptions import TusCommunicationError

from helpers import upload_file_to_supabase
from helpers import get_batches
from helpers import upload_file_to_mistral_ocr
from helpers import insert_page_to_supabase
from helpers import create_embedding

# ----- Setup stuff
dotenv.load_dotenv()

supabase_url: str = st.secrets["SUPABASE_URL"]
supabase_key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(supabase_url, supabase_key)

mistral_api_key = st.secrets["MISTRAL_API_KEY"]
client = Mistral(api_key=mistral_api_key)

industry_sector = pd.read_csv("sasb-industry-sector.csv")


# ----- Initialize session state for user if not already present
st.set_page_config(page_title="Report Manager | SRN", page_icon="logo.png")
st.markdown("""<style> footer {visibility: hidden;} </style> """, unsafe_allow_html=True)

# ----- Login Function (placed in the sidebar)
if "user" not in st.session_state:
    st.session_state["user"] = None

def login_sidebar():
    with st.sidebar.form(key="login_form", enter_to_submit=False):
        st.subheader("Log in")
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")
        submit_button = st.form_submit_button(label="Log In")
    
    if submit_button:
        try:
            response = supabase.auth.sign_in_with_password({"email": email, "password": password})
            user = response.user
            if user:
                st.session_state["user"] = user  # Save user session
                st.session_state["session"] = response.session
                st.sidebar.success("Logged in successfully!")
        except:
            st.sidebar.error("Login failed. Please check your credentials.")
        
        st.rerun()
        

# ----- Logout Function (also in the sidebar)
def logout_sidebar():
    st.session_state["user"] = None
    st.sidebar.success("You have been logged out.")
    st.rerun()

# ----- Main UI

# In the sidebar, display login form or user details
if st.session_state["user"] is None:
    login_sidebar()

else:
    st.sidebar.write("You are logged in as " + st.session_state["user"].email)

    if st.sidebar.button("Log Out"):
        logout_sidebar()

    with st.sidebar:
        st.divider()
        pdf_is_cut = st.checkbox(label="PDF is already sliced")
        if pdf_is_cut:
            st.caption("You still have to enter the correct start and end pages in the PDF!")
        
        # batch_size = st.number_input(label="Batch size for PDF pages", min_value=1, value=50, disabled=pdf_is_cut)
        batch_size = 50

# Main content of the app
st.title("CSRD reports database")


if st.session_state["user"]:

    st.markdown("## Add a new report")
    st.caption("Add a new report for a company. Checks if company exists (based on Name) and if not, creates a new one.")

    companyName = st.text_input("Name")
    companyIsin = st.text_input("ISIN")
    companyIndustry = st.selectbox("Industry", options=sorted(industry_sector['industry'].values), index=0)
    companyCountry = st.text_input("Country")
    
    leftCol3, leftCol4 = st.columns(2)
    with leftCol3:
        documentYear = st.number_input("Year", step=1, value=2024)
    with leftCol4:
        documentType = st.selectbox("Type", options=["Annual Report", "Sustainability Report", "Other"])

    uploaded_file = st.file_uploader("Upload PDF", accept_multiple_files=False, type="PDF")

    leftCol3, leftCol4 = st.columns(2)
    with leftCol3:
        startPdf = st.number_input(step=1, min_value=1, value=None, label="Start page in the PDF file")
    with leftCol4:  
        endPdf = st.number_input(step=1, min_value=2, value=None, label="Ending page in the PDF file")

    submit = st.button("Process PDF", disabled=companyName=="" or startPdf==0 or endPdf==0)



    if submit and uploaded_file is not None:

        # Reset auth sesssion
        supabase.auth.set_session(
            access_token=st.session_state.get("session").access_token,
            refresh_token=st.session_state.get("session").refresh_token
            )
        
        # Upsert company first
        try:
            companyUpsert_response = (
                supabase.table("companies")
                .upsert(
                    {
                        "name": companyName.strip(),
                        "isin": companyIsin.strip(), 
                        "country": companyCountry.strip(),
                        "industry": companyIndustry,
                        "sector": industry_sector.query("industry == @companyIndustry")['sector'].values[0],
                    }, 
                    on_conflict=["name"]
                )
                .execute()
            )
            st.toast(f"Upserted {companyName} as a company", icon=":material/check:")

        except Exception as e:
            st.error(e)

        # Handle PDF
        if not pdf_is_cut:
            pdf_data = uploaded_file.read()
            doc = pymupdf.open(stream=pdf_data)
            doc.select(list(range(startPdf - 1, min(endPdf, len(doc)))))
            doc.save("sliced-pdf.pdf")
            st.toast(f"Sliced pages from PDF", icon="🔪")

        else:
            with open("sliced-pdf.pdf", mode='wb') as w:
                w.write(uploaded_file.getvalue())

        # Upsert document to database
        documentUpsert_response = (
            supabase.table("documents")
            .upsert(
                {
                    "company_id": companyUpsert_response.data[0].get("id"),
                    "year": documentYear,
                    "type": documentType,
                    "pages": f"({startPdf}, {endPdf})"
                },
                on_conflict=["company_id, year, type"]
            )
            .execute()
        )
        st.toast(f"Upserted document", icon="🎉")
        document_id = documentUpsert_response.data[0].get("id")

        # Upload PDF file to CDN
        try:
            with open("sliced-pdf.pdf", "rb") as fs:
                upload_file_to_supabase(
                    supabase_url=supabase_url,
                    file_name=document_id + ".pdf",
                    file=fs,
                    access_token=st.session_state.get("session").access_token,
                )
            st.toast("File uploaded to CDN!", icon="🍿")
            
        except Exception as e:
            if isinstance(e, TusCommunicationError):
                st.toast("File exists on CDN, proceeding...", icon="🤷")
            else:
                st.info(f"Could not upload PDF to CDN ({e})\nWill continue processing the pages...")


        if not pdf_is_cut:
            # Upload batches of pages of the PDF to Mistral and create the embeddings
            for batch, page_range in enumerate(get_batches(len(doc), batch_size=batch_size)):
                start = list(page_range)[0]
                end = list(page_range)[-1]
                doc = pymupdf.open("sliced-pdf.pdf")
                doc.select(list(page_range))
                doc.save("sliced-pdf-pages.pdf")
                
                ocr_response = upload_file_to_mistral_ocr("sliced-pdf-pages.pdf", mistral_api_key)
                text_markdowns = [x.markdown for x in ocr_response.pages]

                embedding_response = create_embedding(text_markdowns, mistral_api_key)
                text_embeddings = [x.embedding for x in embedding_response.data]

                for p, (markdown, embedding) in enumerate(zip(text_markdowns, text_embeddings)):
                    real_page = startPdf + start + p
                    insert_page_to_supabase(supabase, document_id, real_page, markdown, embedding)
                    

                st.toast(f"Added markdown and embeddings to database (batch {batch + 1})", icon="🤙")

        else:
            ocr_response = upload_file_to_mistral_ocr("sliced-pdf.pdf", mistral_api_key)
            text_markdowns = [x.markdown for x in ocr_response.pages]

            for batch, page_range in enumerate(get_batches(len(ocr_response.pages), batch_size=batch_size)):
                start = list(page_range)[0]
                end = list(page_range)[-1]
                text_markdowns_relevant = text_markdowns[start:end+1]

                embedding_response = create_embedding(text_markdowns_relevant, mistral_api_key)
                text_embeddings = [x.embedding for x in embedding_response.data]

                for p, (markdown, embedding) in enumerate(zip(text_markdowns_relevant, text_embeddings)):
                    real_page = startPdf + start + p
                    insert_page_to_supabase(supabase, document_id, real_page, markdown, embedding)

                st.toast(f"Added markdown and embeddings to database (batch {batch + 1})", icon="🤙")


else:
    st.write("You need to log in to access and manage our document database.")