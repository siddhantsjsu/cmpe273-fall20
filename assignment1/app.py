from flask import Flask, request, abort, send_file, make_response
import flask_monitoringdashboard as dashboard
from sqlitedict import SqliteDict
import hashlib 
import qrcode
from io import BytesIO
bookmarksDB = SqliteDict('./my_db.sqlite')

app = Flask(__name__)
dashboard.bind(app)

@app.route('/api/bookmarks', methods = ['POST'])
def createBookmark():
    encodedUrl = request.json['url'].encode("utf-8")
    hashedUrl = hashlib.md5(encodedUrl).hexdigest()
    if hashedUrl in bookmarksDB:
        resp = {"reason": "The given URL already existed in the system."}
        return resp, 400
    else:
        reqDict = request.get_json()
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
        resp['readCount'] += 1
        ETag = hashlib.md5((resp['name'] + resp['url'] + str(resp['readCount'])).encode("utf-8")).hexdigest()
        resp['ETag'] = ETag
        bookmarksDB[id] = resp
        bookmarksDB.commit()
        resp.pop('readCount')
        resp.pop('ETag')
        return resp
        
@app.route('/api/bookmarks/<id>', methods = ['DELETE'])
def deleteBookmark(id):
    if id not in bookmarksDB:
        abort(404)
    else:
        bookmarksDB.pop(id)
        bookmarksDB.commit()
        return '', 204

@app.route('/api/bookmarks/<id>/qrcode', methods = ['GET'])
def getBookmarkQRcode(id):
    if id not in bookmarksDB:
        abort(404)
    else:
        bookmark = bookmarksDB.get(id)
        img = qrcode.make(bookmark.get('url'))
        buf = BytesIO()
        img.save(buf, format = 'JPEG')
        buf.seek(0)
        return send_file(buf, mimetype="image/jpeg", attachment_filename="qrcode.jpg", as_attachment=True)

@app.route('/api/bookmarks/<id>/stats', methods = ['GET'])
def getBookmarkStats(id):
    if id not in bookmarksDB:
        abort(404)
    else:
        if request.headers.get('ETag') == None or request.headers.get('ETag') == '':
            bookmark = bookmarksDB.get(id)
            resp = make_response(str(bookmark.get('readCount')), 200)
            resp.headers['ETag'] = bookmark.get('ETag')
            return resp
        else:
            bookmark = bookmarksDB.get(id)
            if request.headers.get('ETag') == bookmark.get('ETag'):
                resp = make_response('', 304)
                resp.headers['ETag'] = bookmark.get('ETag')
                return resp
            else:
                resp = make_response(str(bookmark.get('readCount')), 200)
                resp.headers['ETag'] = bookmark.get('ETag')
                return resp
