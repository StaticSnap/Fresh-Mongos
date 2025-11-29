import streamlit as st
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# 1. DATABASE CONNECTION AND SETUP
# Connects to the database and keeps the connection open.
@st.cache_resource
def connect_to_database():
    try:
        # Tries to connect to MongoDB on the default port.
        db_client = MongoClient("mongodb://localhost:27017")
        # Checks the connection status.
        db_client.admin.command('ping') 
        return db_client
    except Exception as e:
        st.error(f"Cannot connect to MongoDB. Please start the server (mongod) first: {e}")
        return None

mongo_client = connect_to_database()

if mongo_client:
    db = mongo_client["YoutubeData"]
    video_collection = db["Video"] # Main collection reference
else:
    # Stops the application if the database connection fails.
    st.stop()

# 2. UI SETUP AND NAVIGATION
st.set_page_config(page_title="YouTube Data Analytics", layout="wide")
st.title("📺 YouTube Data Engine (Milestone 4)")

# Sets up the main menu in the sidebar.
st.sidebar.header("Menu")
current_page = st.sidebar.radio("Select View:", ["Dashboard & Charts", "Search Videos", "Scalable Analytics"])

# 3. DASHBOARD MODULE (Data Visualization)
if current_page == "Dashboard & Charts":
    st.header("📊 Data Dashboard")
    
    # Quick Metric: Total video count
    total_videos = video_collection.count_documents({})
    st.metric(label="Total Videos Loaded", value=f"{total_videos:,}")

    # Chart 1: Category Counts
    st.subheader("Distribution by Category")
    
    # MongoDB aggregation to count videos per category.
    count_pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    category_data = list(video_collection.aggregate(count_pipeline))
    
    if category_data:
        df_categories = pd.DataFrame(category_data)
        df_categories.rename(columns={"_id": "Category", "count": "Count"}, inplace=True)
        
        # Bar chart visualization
        st.bar_chart(df_categories.set_index("Category"))
    else:
        st.warning("No data found in MongoDB.")

    # Chart 2: Views vs Rating Scatter Plot
    st.subheader("Views vs. Rating")
    st.write("Correlation between rating and view count (Uses a 1000-record sample)")
    
    # Fetches a random sample for fast plotting.
    sample_records = list(video_collection.find({}, {"views": 1, "rating": 1, "category": 1, "_id": 0}).limit(1000))
    if sample_records:
        df_scatter = pd.DataFrame(sample_records)
        
        # Plots the scatter graph.
        fig, ax = plt.subplots()
        ax.scatter(df_scatter['rating'], df_scatter['views'], alpha=0.5, c='blue')
        ax.set_xlabel("Rating")
        ax.set_ylabel("Views")
        st.pyplot(fig)

        
# 4. SEARCH MODULE (Interactive Query)
elif current_page == "Search Videos":
    st.header("🔍 Interactive Search")
    
    # Radio button for selecting search criteria.
    search_criteria = st.radio("Search by:", ["Video ID", "Uploader Name"])
    
    if search_criteria == "Video ID":
        video_id_input = st.text_input("Enter Video ID (e.g., yZIkFwxLUeU)")
        if st.button("Search Video"):
            # Queries MongoDB for a single video record.
            record_result = video_collection.find_one({"videoID": video_id_input.strip()})
            
            if record_result:
                st.success(f"Record Found: {record_result.get('videoID')}")
                
                # Prepares data for a clean table display.
                display_data = {
                    "Metric": ["Uploader", "Category", "Duration (sec)", "Views", "Rating", "Related Videos Count"],
                    "Value": [
                        record_result.get('uploader'),
                        record_result.get('category'),
                        record_result.get('duration'),
                        f"{record_result.get('views'):,}",
                        f"{record_result.get('rating')}",
                        len(record_result.get('related', []))
                    ]
                }
                
                # Displays the data in a user-friendly table format.
                df_friendly = pd.DataFrame(display_data)
                st.table(df_friendly.set_index('Metric'))
                
                # Shows related IDs in a collapsible section.
                with st.expander("View Full List of Related Video IDs"):
                    st.write(", ".join(record_result.get('related', [])))

            else:
                st.error("Video ID not found in the database.")
                
    elif search_criteria == "Uploader Name":
        uploader_input = st.text_input("Enter Uploader Name")
        if st.button("Search Uploader"):
            # Queries MongoDB for multiple videos by the uploader (limited to 5).
            uploader_results = list(video_collection.find({"uploader": uploader_input.strip()}).limit(5))
            if uploader_results:
                st.write(f"Found {len(uploader_results)} videos (showing top 5):")
                
                # Iterates and displays results in expandable sections.
                for video in uploader_results:
                    with st.expander(f"{video.get('category')} - {video.get('videoID')}"):
                        st.write(f"Views: {video.get('views')}")
                        st.write(f"Rating: {video.get('rating')}")
                        st.write(f"Related Videos: {video.get('related')}")
            else:
                st.error("Uploader not found.")

# 5. SPARK ANALYTICS MODULE (Scalable Processing)
elif current_page == "Scalable Analytics":
    st.header("⚡ Scalable Data Processing")
    st.info("This section queries the database based on results that would typically come from a MapReduce job.")
    
    st.subheader("Algorithm: Reverse Related Search")
    target_id = st.text_input("Enter Target Video ID to find videos that reference it", "yZIkFwxLUeU")
    
    if st.button("Run Reverse Index Check"):
        # Queries Mongo using the 'related' array index. This mimics the core MapReduce logic 
        # (finding all records that point to a specific key).
        pointing_to_me = list(video_collection.find({"related": target_id}))
        
        if pointing_to_me:
            st.write(f"Found {len(pointing_to_me)} videos that recommend **{target_id}**:")
            
            # Displays results in a simple table.
            df_related = pd.DataFrame(pointing_to_me)
            st.dataframe(df_related[['videoID', 'uploader', 'category']])
        else:
            st.warning(f"No videos found that list {target_id} as related.")

""" 
Pseudocode:

BEGIN PROGRAM

1. CONNECT TO DATABASE
   FUNCTION connect_to_database():
       TRY
           connect to MongoDB at localhost:27017
           run ping command to verify connection
           RETURN database client
       CATCH error
           show error message in UI
           RETURN null

   mongo_client = connect_to_database()

   IF mongo_client is null:
       stop program
   ELSE:
       db = mongo_client["YoutubeData"]
       video_collection = db["Video"]

2. SETUP USER INTERFACE
   configure page with title "YouTube Data Analytics"
   show main title "YouTube Data Engine (Milestone 4)"

   sidebar menu:
       options = ["Dashboard & Charts", "Search Videos", "Scalable Analytics"]
       current_page = user selection

3. DASHBOARD & CHARTS MODULE
   IF current_page == "Dashboard & Charts":
       show header "Data Dashboard"

       // Metric: total videos
       total_videos = count all documents in video_collection
       display metric "Total Videos Loaded" = total_videos

       // Chart 1: Category Counts
       run aggregation pipeline:
           group by category, count videos
           sort by count descending
           limit to top 10
       category_data = results

       IF category_data not empty:
           convert to table (Category, Count)
           show bar chart of counts per category
       ELSE:
           show warning "No data found"

       // Chart 2: Views vs Rating Scatter Plot
       show subheader "Views vs Rating"
       explain correlation plot

       sample_records = fetch up to 1000 documents with fields (views, rating, category)
       IF sample_records not empty:
           convert to table
           plot scatter chart: x=rating, y=views
           show chart

4. SEARCH VIDEOS MODULE
   ELSE IF current_page == "Search Videos":
       show header "Interactive Search"

       search_criteria = user chooses ["Video ID", "Uploader Name"]

       IF search_criteria == "Video ID":
           video_id_input = user text input
           IF user clicks "Search Video":
               record_result = find one document where videoID = input
               IF record_result exists:
                   show success message
                   prepare table with metrics:
                       uploader, category, duration, views, rating, related count
                   display table
                   expandable section: show list of related video IDs
               ELSE:
                   show error "Video ID not found"

       ELSE IF search_criteria == "Uploader Name":
           uploader_input = user text input
           IF user clicks "Search Uploader":
               uploader_results = find up to 5 documents where uploader = input
               IF results exist:
                   show message "Found N videos"
                   FOR each video in results:
                       expandable section with:
                           category + videoID
                           views, rating, related videos
               ELSE:
                   show error "Uploader not found"

5. SCALABLE ANALYTICS MODULE
   ELSE IF current_page == "Scalable Analytics":
       show header "Scalable Data Processing"
       show info message about MapReduce-like query

       show subheader "Reverse Related Search"
       target_id = user text input (default example ID)

       IF user clicks "Run Reverse Index Check":
           pointing_to_me = find all documents where 'related' array contains target_id
           IF results exist:
               show message "Found N videos that recommend target_id"
               convert results to table with columns (videoID, uploader, category)
               display table
           ELSE:
               show warning "No videos found that list target_id"

END PROGRAM """