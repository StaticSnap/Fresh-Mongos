import json
from pymongo import MongoClient
import os

c = MongoClient('mongodb://localhost:27017')
db = c['YoutubeData']
col = db['Video']

out = {}
# 1. total count
out['total_count'] = col.count_documents({})

# 2. top 10 categories
pipeline = [
    {'$group': {'_id': '$category', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}},
    {'$limit': 10}
]
out['top_categories'] = list(col.aggregate(pipeline))

# 3. sample records with non-null rating and views (limit 1000)
sample_cursor = col.find({'rating': {'$ne': None}, 'views': {'$ne': None}}, {'views':1,'rating':1,'category':1,'videoID':1}).limit(1000)
sample_list = list(sample_cursor)
out['sample_count'] = len(sample_list)
out['sample_head'] = sample_list[:5]

# 4. find specific video
video_id = 'yZIkFwxLUeU'
out['lookup_video'] = col.find_one({'videoID': video_id})

# 5. find uploader
uploader = 'dudeski0000'
out['uploader_results'] = list(col.find({'uploader': uploader}).limit(5))

# 6. reverse related lookup
out['reverse_lookup_count'] = col.count_documents({'related': video_id})
out['reverse_lookup_head'] = list(col.find({'related': video_id}, {'videoID':1,'uploader':1,'category':1}).limit(20))

# 7. file-based: count lines in cleanData files
clean_folder = os.path.join(os.getcwd(), 'cleanData')
file_counts = {}
if os.path.isdir(clean_folder):
    for fn in os.listdir(clean_folder):
        path = os.path.join(clean_folder, fn)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                file_counts[fn] = sum(1 for _ in f)
        except Exception as e:
            file_counts[fn] = f'error: {e}'
out['cleanfile_line_counts'] = file_counts

print(json.dumps(out, indent=2, default=str))