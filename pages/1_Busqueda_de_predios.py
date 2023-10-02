import streamlit as st
from _predios import main

st.set_page_config(layout='wide',initial_sidebar_state="collapsed",page_icon ="https://www.cbre.com/-/media/project/cbre/dotcom/global/unsorted/favicon_lg.png?rev=f6bed35a1dfb4fac9ede077cef213618")
#st.set_page_config(layout='wide')

main()