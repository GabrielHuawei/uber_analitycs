import streamlit as st
import plotly.express as px
import pandas as pd
from io import StringIO




# payment_type filter
payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
}

rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}

st.set_page_config(
    page_title="Taxi Driveway Analisys",
    page_icon=":bar_chart:",
    layout="wide"
)

# with open(r'assets\fact_json.json', 'r') as file:
#     data = json.load(file)
#     df = pd.DataFrame(data['fact_table'])
#     print(df)
df = pd.read_csv(r'assets\uber_refined_data.csv')
df = df.rename(columns={'dropoff_latitude': 'latitude', 'dropoff_longitude': 'longitude'})

print(df)


# st.dataframe(df)


# --- SIDEBAR --- 
st.sidebar.header('Please Filter Here')


uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
    bytes_data = uploaded_file.getvalue()
    st.write(bytes_data)

    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
    st.write(stringio)

    string_data = stringio.read()
    st.write(string_data)

    dataframe = pd.read_csv(uploaded_file)
    st.write(dataframe)

id = st.sidebar.multiselect(
    "Select the Trip Id:",
    options=df['trip_id'].unique(),
    default=df['trip_id'].unique()[:10]
)

payment_type_name = st.sidebar.multiselect(
    "Select the Payment Type:",
    options=df['payment_type_name'].unique(),
    default=df['payment_type_name'].unique()
)

rate_code_map = st.sidebar.multiselect(
    "Select the Rate Codes:",
    options=df['rate_code_name'].unique(),
    default=df['rate_code_name'].unique()
)

trip_distance = st.sidebar.number_input('Pick a Distance Number:', 0., df['trip_distance'].max(),
                                        step=1.,
                                        format="%.2f",
                                        )

passenger_count = st.sidebar.number_input('Number of Passagers:', 0, df['passenger_count'].max())

df_selection = df.query(
    "payment_type_name == @payment_type_name & trip_distance == @trip_distance & rate_code_name == @rate_code_map"
)
# main page

st.title(':card_index_dividers: Uber Data Analysis')
st.markdown('##')

st.dataframe(df_selection)

st.markdown('##')


total_amount = df_selection['total_amount'].sum()
avarage_tips_amount = df_selection['tip_amount'].mean()
avarage_fare_amount = df_selection['total_amount'].mean()

left_column, middle_column, right_column = st.columns(3)

with left_column:
    st.subheader('Total Amount:')
    st.subheader(f"US $ {total_amount:,}")

with middle_column:
    st.subheader('Avarage Tip Amount:')
    st.subheader(f"US $ {avarage_tips_amount:.2f}")    

with right_column:
    st.subheader('Avarage Fare Amount')
    st.subheader(f"US $ {avarage_fare_amount:.2f}")    

st.markdown('##')

st.map(df)