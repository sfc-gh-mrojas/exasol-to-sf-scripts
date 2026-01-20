# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import io
from pathlib import Path
import os

# Write directly to the app
st.title(f"Exasol Lua Acelerator")

if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0
if "input_files" not in st.session_state:
    st.session_state.input_files = {}
if "output_files" not in st.session_state:
    st.session_state.output_files = {}

DESCRIPTION = """

## 🧩 **App Overview**

### 🎯 **Purpose**

This Streamlit app is designed to help developers **migrate legacy scripts (written in Exasol SQL, Lua, Java, or Python)** into **Snowflake Snowpark Python stored procedures**. It integrates with **Snowflake Cortex ** to automatically rewrite code using AI, making the modernization process faster and more consistent.

---

### 🧱 **Core Features**

| Feature                               | Description                                                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **1. Script Migration Assistant**     | Converts legacy scripts into Snowpark-compatible Python stored procedures using AI (Snowflake Cortex).                    |
| **2. File Upload to Snowflake Stage** | Allows users to upload files directly into a selected Snowflake internal stage.                                           |
| **3. File Viewer with Migration UI**  | Lists and previews files stored in a Snowflake stage. Lets users select files, view contents, and run migrations.         |
| **4. Customizable Prompt Template**   | Users can adjust the AI prompt template that controls how scripts are translated.                                         |

---

### 🧭 **Main Components**

#### 1. **Sidebar**

* **Navigation** between three pages: Home, File Viewer, and Settings.
* **Stage selector** to choose a Snowflake stage for uploading or viewing files.
* **File uploader** to upload scripts to the selected stage.

#### 2. **Home Page**

* A simple welcome message and entry point to the app.

#### 3. **File Viewer Page**

* Lists files from the selected Snowflake stage.
* Displays file metadata and allows selecting files for migration.
* Shows the original and migrated code side-by-side for each file.
* Triggers AI-assisted conversion via Snowflake Cortex on demand.

#### 4. **Settings Page**

* Provides a large text area to customize the **default migration prompt** used by the AI model.

---

### 🧠 **Typical Workflow**

1. **Select a Snowflake stage** in the sidebar.
2. **Upload script files** (Lua/Java/Exasol SQL/Python) via the uploader.
3. **Switch to File Viewer**, refresh the list, and select files to process.
4. **View and migrate** code using Snowflake Cortex with one click.
5. **Review original vs. migrated output** in tabs for each file.
6. Optionally, **customize the AI prompt** in Settings to fine-tune behavior.


"""


if "default_prompt" not in st.session_state:
    st.session_state.default_prompt = """
Rewrite this @@language script as snowpark python proc using an inline handler.
Following a pattern like
CREATE OR REPLACE PROCEDURE MYPROC(ARG1 STRING, ARG2 STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'main'
  EXECUTE AS CALLER
  AS
  $$
from snowflake.snowpark import Session

def main(session: Session, arg1: str, arg2: str) -> str:
    # Your code here
    # You can use session.sql to execute SQL commands
    # For example:
    result = session.sql("SELECT * FROM my_table").collect()
    return str(result)
  $$;

call to pquery or query can be replaced by session.sql. 
The SQL code statements were written for EXASOL, so you need to adapt them to Snowflake SQL syntax.
For example EXASOL EXA_SYSCAT is replaced by INFORMATION_SCHEMA.
If possible keep the comments about the original script in the new script.
For python scripts if numpy is used add the package numpy to the PACKAGES list.
Expressions like error("message") should be replaced by raise Exception("message").
==== CODE ====
@@code
    """

def get_prompt(language, code):
    template = st.session_state.default_prompt
    return template.replace("@@language",language).replace("@@code",code)

def determine_script_type(lines):
    for line in lines:
        if "CREATE JAVA" in line:
            return "java"
        if "CREATE LUA" in line:
            return "lua"
        if "CREATE PYTHON" in line:
            return "python"
    return ""

def reset_update_key():
    st.session_state.uploader_key += 1

def decode_with_fallback(byte_content: bytes) -> str:
    """
    Decode bytes to string with encoding fallback for non-UTF-8 files.
    Tries multiple encodings and falls back to replacement characters if all fail.
    """
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
    
    for encoding in encodings_to_try:
        try:
            return byte_content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    
    # Last resort: decode with replacement characters
    return byte_content.decode('utf-8', errors='replace')
    
def get_stages():
    return [x[0] for x in get_active_session().sql("show stages").select('"name"').collect()]


def retrieve_files():
    if st.session_state.input_stage:
        df = get_active_session().sql(f"list @{st.session_state.input_stage}").to_pandas()
    else:
        st.warning("Please select an input stage")
        df = pd.DataFrame([],columns=["name","size","md5","last_modified"])
    df.columns = ["name","size","md5","last_modified"]
    return df




        
# Define your "pages" as functions
def home():
    st.write(DESCRIPTION)

def settings():
    st.title("⚙️ Settings")
    st.write("Adjust your app settings here.")
    st.session_state.default_prompt=st.text_area("Default Prompt",height=800,
    value=st.session_state.default_prompt)

def file_viewer():
    if st.button("Refresh File List"):
        st.session_state.df_files = retrieve_files()

    if "df_files" in st.session_state:
        df_files = st.session_state.df_files.copy()
    
        # Ensure the "Select" column exists and is boolean
        if "Select" not in df_files.columns:
            df_files["Select"] = False
    
        # Reorder columns to make "Select" the first
        cols = ["Select"] + [col for col in df_files.columns if col != "Select" and col != "md5"]
        df_files = df_files[cols]
        
        if len(df_files):
            # Disable editing for all columns except "Select"
            disabled_cols = [col for col in df_files.columns if col != "Select"]
            
            edited_df = st.data_editor(
                df_files,
                hide_index=True,
                column_config={"Select": st.column_config.CheckboxColumn(required=True)},
                disabled=disabled_cols,
            )
    
            if len(edited_df):
                st.session_state.selected_files = edited_df[edited_df.Select]
            else:
                st.session_state.selected_files = None
    
        else:
            st.warning("No files found")
    
    if "selected_files" in st.session_state:
        input_files  = st.session_state.input_files
        output_files = st.session_state.output_files 
        with st.spinner("retriving files...",show_time=True):
            
            for row in st.session_state.selected_files.itertuples():
                file_name = row[2]
                file_bytes = get_active_session().file.get_stream('@' + file_name).read()
                input_files[file_name] = decode_with_fallback(file_bytes)
            st.session_state.input_files = input_files

        if st.button("Migrate"):
            for row in st.session_state.selected_files.itertuples():
                file_name = row[2]
        for row in st.session_state.selected_files.itertuples():
            file_name     = row[2]
            original_code = input_files.get(file_name,"")
            type          = determine_script_type(original_code.splitlines())
            with st.expander(file_name):
                original, migrated = st.tabs(["original","migrated"])
                with original:
                    st.code(original_code)
                with migrated:
                    if st.button("Migrate",key=f"btn_mig_{file_name}"):
                        with st.spinner("Migrating...",show_time=True):
                            prompt = get_prompt(type, original_code)
                            response_rows = get_active_session().sql("select SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',?)", params=[prompt]).collect()
                            response = response_rows[0][0] if response_rows else "No response received."
                            output_files[file_name] = response
                    migrated_code = output_files.get(file_name, "")
                    if migrated_code:
                        st.code(migrated_code, language="python")
                    else:
                        st.info("Click 'Migrate' to convert this script to Snowpark Python.")


with st.sidebar:
    page = st.sidebar.radio("Go to", ["Home", "File Viewer", "Settings"])
        
    st.session_state.input_stage = st.selectbox("input_stage",get_stages())
    uploaded_files = st.file_uploader("Choose a file", accept_multiple_files=True, key=f"uploader_{st.session_state.uploader_key}")
    if uploaded_files and st.button("Upload"):
        for uploaded_file in uploaded_files:
            try:
    
                # Create file stream using BytesIO and upload
                file_stream = io.BytesIO(uploaded_file.getvalue())
                get_active_session().file.put_stream(
                    file_stream,
                    f"{st.session_state.input_stage}/{uploaded_file.name}",
                    auto_compress=False,
                    overwrite=True
                )
                st.success(f"File '{uploaded_file.name}' has been uploaded successfully!")
            except:
                st.error(f"Error with '{uploaded_file.name}' !")
        reset_update_key()

# Routing logic
if page == "Home":
    home()
elif page == "File Viewer":
    file_viewer()
elif page == "Settings":
    settings()
