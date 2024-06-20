import streamlit as st
from pipeline import pipeline

st.title('Files processor')

if st.button('Process'):
    with st.spinner('Processing...'):
        logs = pipeline()

        for log in logs:
            st.write(log)
