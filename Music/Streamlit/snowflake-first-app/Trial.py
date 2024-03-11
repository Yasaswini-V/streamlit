import streamlit as st
import pandas as pd

# Function to get changed rows
def get_changed_rows(df_old, df_new):
    changed_rows = []
    for index,row in df_new.iterrows(): 
        if not row.equals(df_old.loc[index]):
            changed_rows.append(row)
    return changed_rows

# Main code
df_old = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C':[7,8,9]})

st.write('Original Data:')
st.dataframe(df_old)

# Data editor
df_new = st.data_editor(df_old)

# Check for changes
changed_rows=get_changed_rows(df_old,df_new)
# st.write(changed_rows)
if changed_rows:
    st.write('Changed Rows:')
    st.write(pd.DataFrame(changed_rows))
else:
    st.write('No changes detected.')
