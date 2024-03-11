def edit(df):
    editor=st.data_editor(df)
    sub,wt,buff=st.columns([1,1,2])
    save=sub.button('Save')
    write=wt.button('Write to SF',key='edit_write')
    if save :
        df=editor
        st.write('Edited Table:')
        st.dataframe(df)
    elif write:
        df=editor
        Update_table(df)
        st.dataframe(df)