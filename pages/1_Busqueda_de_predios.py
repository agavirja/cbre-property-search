import streamlit as st
from _predios import main

st.set_page_config(layout='wide',initial_sidebar_state="collapsed")
#st.set_page_config(layout='wide')

main()