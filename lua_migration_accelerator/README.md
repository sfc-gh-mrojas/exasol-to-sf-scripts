
## 🧩 **App Overview: Exasol Lua Accelerator**

### 🎯 **Purpose**

This Streamlit app is designed to help developers **migrate legacy scripts (written in Exasol SQL, Lua, Java, or Python)** into **Snowflake Snowpark Python stored procedures**. It integrates with **Snowflake Cortex ** to automatically rewrite code using AI, making the modernization process faster and more consistent.

---

## ⚙️ **Installation & Deployment (on Snowflake)**

To deploy this app in Snowflake’s **Native Streamlit Hosting**, follow these steps:

### 🧾 **Step-by-Step Guide**

1. ### ✅ **Create a Python file**

   Save the contents of your app (i.e., the provided `lua_migration_accelerator.py`) locally.

   Open on a local editor select all the contents.

2. ### 📦 **Upload to Snowflake**

   Open your Snowflake **Snowsight console**, and go to **Apps > Streamlit**. Click **+ Streamlit App**.

3. ### 🧭 **Configure the App**

   * **Name:** Choose a descriptive name, like `lua_migration_accelerator`

   You need to specify a database, schema and a warehouse
   

4. ### 🔐 **Permissions**

    In order to upload files you need to grant your role access to the Snowflake stage used by the app.

5. ### 🚀 **Launch the App**

   After deployment, click **Open Streamlit App**. You can now upload files, select them, and use the AI-assisted migration feature.

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
