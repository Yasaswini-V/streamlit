
from streamlit.web.server.websocket_headers import _get_websocket_headers
headers = _get_websocket_headers()
st.write(headers)
if "X-Ms-Client-Principal-Name" in headers:
    user_email = headers["X-Ms-Client-Principal-Name"]
    st.write(headers) 

    