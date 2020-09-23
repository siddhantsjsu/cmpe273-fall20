from flask import Flask, request, abort
from sqlitedict import SqliteDict
import hashlib
bookmarksDB = SqliteDict('./my_db.sqlite')

app = Flask(__name__)

@app.route('/api/bookmarks', methods = ['POST'])
def createBookmark():
    encodedUrl = request.json['url'].encode("utf-8")
    hashedUrl = hashlib.md5(encodedUrl).hexdigest()
    if hashedUrl in bookmarksDB:
        resp = {"reason": "The given URL already existed in the system."}
        return resp, 400
    else:
        reqDict = request.get_json()
        # print(reqDict)
        reqDict['readCount'] = 0
        bookmarksDB[hashedUrl] = reqDict
        bookmarksDB.commit()
        resp = { "id": hashedUrl }
        return resp

@app.route('/api/bookmarks/<id>', methods = ['GET'])
def getBookmark(id):
    if id not in bookmarksDB:
        abort(404)
    else:
        resp = bookmarksDB.get(id)
        # print(resp)
        resp['readCount'] += 1
        bookmarksDB[id] = resp
        bookmarksDB.commit()
        resp.pop('readCount')
        # print(bookmarksDB.get(id))
        return resp
        
    

