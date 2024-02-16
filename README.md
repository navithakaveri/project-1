# project-1 YouTube data harvesting and warehousing using python ,SQL, MongoDB

**Connect to the YouTube API:**
 Created a function called api_connect() that connects to the YouTube API using the build() function, which presumably comes from the Google API client library. 
 It is used  to retrieve channel and video data. It can use the Google API client library for Python to make requests to the API.
 
 **Retrieves information** 
*Retrieves information about a YouTube channel specified by its ID, including its name, ID, subscriber count, view count, total videos, description, and uploads playlist ID.
*Retrieves the IDs of videos uploaded to a YouTube channel specified by its ID.
*Retrieves information about YouTube videos specified by their IDs, including details like the channel name, ID, title, thumbnail URL, comment count, like count, dislike count, favorite count, description, published date, duration, view count, definition, and caption status.
*Retrieves comments for videos specified by their IDs.
*Retrieves details of playlists associated with a YouTube channel specified by its ID, including the playlist ID, title, channel ID, channel name, published date, and video count.

**Store data in a MongoDB data** 
Once retrieve the data from the YouTube API, It was stored it in a MongoDB . MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.

**Migrate data to a SQL data warehouse**
After collected data for multiple channels, migrate it to a SQL data warehouse.
creating functions to extract information from a MongoDB database and insert that data into corresponding tables in a MySQL database. 

**Query the SQL data warehouse:**
Building Streamlit functions to display data and query it with predefined questions.

**Set up a Streamlit app** 
Streamlit is a great choice for building data visualization and analysis tools quickly and easily. It can be use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse

Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.





