# coding:utf-8

from flask import *
from werkzeug.utils import secure_filename
import os
import sys 
from hdfs.client import Client
import json


client = Client("http://localhost:50070")

app = Flask(__name__)

#返回目录下的文件
def HDFSlist(client,hdfs_path):
    return client.list(hdfs_path, status=False)

#读取hdfs文件内容,将每行存入数组返回
def read_hdfs_file(client,filename):
    lines = []
    with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
        for line in reader:
            #pass
            #print line.strip()
            lines.append(line.strip())
    return lines

# 上传文件到hdfs
def put_to_hdfs(client, local_path, hdfs_path):
    client.upload(hdfs_path, local_path, cleanup=True)

@app.route('/', methods=['POST', 'GET'])
def upload():
    '''    
    filenames = HDFSlist(client,'/water-quality/dataset/FlumeResult')
    allcontent = []
    allmonthes = []
    allgood = []
    allpoor = []
    for f in filenames:
        #print(f)
        path = '/water-quality/dataset/FlumeResult/'+f
        content = read_hdfs_file(client,path)
        for c in content:
            allcontent.append(c)
    for c in allcontent:
        #print(c)
        if "," in c:
            data = c.split(',')
            if "优" in data[1]:
                allmonthes.append(data[0])
                allgood.append(data[2])
            elif "劣" in data[1]:
                allpoor.append(data[2])
    print(allmonthes)
    print(allgood)
    print(allpoor)
    #print(HDFSlist(client,'/water-quality/dataset/FlumeResult'))
    '''
    if request.method == 'POST':
        f = request.files['file']
        #basepath = os.path.dirname(__file__)  # 当前文件所在路径
        basepath = "/usr/local/water-quality/dataset"
        upload_path = os.path.join(basepath, 'datasource',secure_filename(f.filename))  #注意：没有的文件夹一定要先创建，不然会提示没有该路径
        f.save(upload_path)
        HDFSpath = "/water-quality/dataset/WaterFiles"
        HDFSfile = HDFSpath + "/" + secure_filename(f.filename)
        print(HDFSfile)
        if client.status(HDFSfile, strict=False):
            pass
        else:
            put_to_hdfs(client, upload_path, HDFSpath)
        return redirect(url_for('upload'))
    #return render_template('index.html',allcontent=allcontent)
    return render_template('index.html')

@app.route("/water", methods=["GET"])
def viewdata():
    if request.method == "GET":
        filenames = HDFSlist(client,'/water-quality/dataset/FlumeResult')
        content = []
        allmonthes = []
        allgood = []
        allpoor = []
        for f in filenames:
            '''
            if ".tmp" in f:
                pass
            else:
                #print(f)
                path = '/water-quality/dataset/FlumeResult/'+f
                content = read_hdfs_file(client,path)
            '''
            path = '/water-quality/dataset/FlumeResult/'+f
            content = read_hdfs_file(client,path)
        for c in content:
            #allcontent.append(c)
        #for c in allcontent:
            #print(c)
            if "," in c:
                data = c.split(',')
                if "优" in data[1]:
                    allmonthes.append(data[0])
                    allgood.append(data[2])
                elif "劣" in data[1]:
                    allpoor.append(data[2])
    return jsonify(month = allmonthes,
                   good = allgood,
                   poor = allpoor)
    #return jsonify(Goods_name = [x[0] for x in res],
    #               Goods_inventory = [x[1] for x in res],
    #               Goods_sales_volume = [x[2] for x in res])

if __name__ == '__main__':
    app.run(debug=True)
