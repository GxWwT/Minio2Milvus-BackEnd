import os
import json
import base64
import torch
import streamlit as st
from utils.MinioTools import MinioClient
from utils.MilvusTools import preprocess, insert_embeddings, MilvusClient
from langchain.embeddings import HuggingFaceBgeEmbeddings


minio_client = MinioClient()
milvus_client = MilvusClient()

st.set_page_config(
  page_title="向量库&知识库管理", 
  page_icon="📝", 
  layout="wide", 
  initial_sidebar_state="expanded")

with st.sidebar:
  # 选择知识库
  buckets =  minio_client.list_buckets()
  selected_bucket = st.selectbox(label="***A. 选择知识库：***", options=buckets)
  st.info(f'已选择知识库：{selected_bucket}')

  option = st.radio("***B. 新建/删除知识库：***", ['新建知识库', '删除知识库'])
  if option == '新建知识库':
    new_bucket = st.text_input('输入新知识库名称：', placeholder="Lower case & numbers, no symbols.")
    if st.button('创建知识库'):
      info = minio_client.make_bucket(new_bucket)
      st.toast(info, icon="ℹ️")
  elif option == '删除知识库':
    st.warning(f'是否要删除知识库: {selected_bucket}', icon="⚠️")
    if st.button('删除知识库'):
      info = minio_client.remove_bucket(selected_bucket)
      st.toast(info, icon="ℹ️")
      if not isinstance(info, Exception):
        list_collections = milvus_client.list_collections()
        if selected_bucket in list_collections:
          milvus_client.drop_collection(selected_bucket)
  
  if st.button("刷新"):
    st.toast("刷新成功！", icon="ℹ️")


st.title('📝向量库&知识库管理')


st.subheader('上传文件', divider='rainbow')
# 上传文件
uploaded_files = st.file_uploader('ℹ️上传完成后，点击文件后✖️删除缓存', key=selected_bucket, accept_multiple_files=True)
if uploaded_files:
  for file in uploaded_files:
    result = minio_client.put_object(selected_bucket,file.name,file)
    st.toast(result)


st.subheader('表单', divider='rainbow')
if selected_bucket:
  objects = minio_client.list_objects(selected_bucket)
else:
  objects = []

header = st.columns([1,2.5,1.5])
all_selected = header[0].checkbox(label='全选|取消', label_visibility='hidden')
header[1].write('**文档**')
header[2].write('**向量库**')


try:
  with open('source_pks.json') as f:
    source_pks = json.load(f)
except:
  source_pks = {}
selected_objects = []
for object in objects:
  row = st.columns([1,2.5,1.5])
  selected = row[0].checkbox(object, value=all_selected, label_visibility='hidden')
  if selected:
    selected_objects.append(object)
  row[1].write(object)
  row[2].write('✔️' if f"{selected_bucket}/{object}" in source_pks else '✖️')


refresh, download, add, delete = None, None, None, None
select = st.selectbox(
  '执行操作:', 
  ('下载所选文件', '将所选文件添加至向量库', '将所选文件从知识库&向量库中删除'))
if select == '下载所选文件':
  download = st.button("下载所选文件")
elif select == '将所选文件添加至向量库':
  add = st.button("将所选文件添加至向量库")
elif select == '将所选文件从知识库&向量库中删除':
  delete = st.button("将所选文件从知识库&向量库中删除")


if add:
  @st.cache_resource
  def load_model():
    embeddings = HuggingFaceBgeEmbeddings(
      model_name="BAAI/bge-large-zh",
      model_kwargs={"device": "cuda" if torch.cuda.is_available() else "cpu"},
      encode_kwargs={"normalize_embeddings": True},
      cache_folder = os.path.join(os.path.dirname(__file__), "embeddings"),
      )
    return embeddings
  embeddings = load_model()
  new_source_pks = {}
  for object in selected_objects:
    file_path =  os.path.join(os.environ['USERPROFILE'], 'Downloads', object)
    response = minio_client.fget_object(selected_bucket, object, file_path)
    if isinstance(response, Exception):
      st.error(response, icon="🚨")
    else:
      source = f"{selected_bucket}/{object}"
      documents = preprocess(file_path, source)
      os.remove(file_path)
      if isinstance(documents, ValueError):
        st.error(documents, icon="🚨")
      else:
        st.toast(f'{object} \n向量化加载中，请等待！')
        emb = insert_embeddings(documents, embeddings, selected_bucket)
        if isinstance(emb, Exception):
          st.error(emb, icon="🚨")
        else:
          source_pk = milvus_client.query(selected_bucket, source)
          new_source_pks.update(source_pk)
          st.toast(f'{object} \n向量化加载成功!', icon='🎉')
  if new_source_pks:
    source_pks.update(new_source_pks)
    with open('source_pks.json', 'w') as f:
      json.dump(source_pks, f)


if delete:
  for object in selected_objects:
    response = minio_client.remove_object(selected_bucket, object)
    st.toast(response)
    if not isinstance(response, Exception):
      source = f"{selected_bucket}/{object}"
      if source in source_pks:
        milvus_client.delete(selected_bucket, source_pks[source])
        source_pks.pop(source)
  with open('source_pks.json', 'w') as f:
    json.dump(source_pks, f)


if download:
  for object in selected_objects:
    data = minio_client.get_object(selected_bucket, object) # return data is bytes-like, data.decode() to str.
    if isinstance(data, bytes):
      base64_str = base64.b64encode(data).decode()
      download_link = f'<a href="data:application/octet-stream;base64,{base64_str}" download={object}>Click to download: {object}</a>'
      st.markdown(download_link, unsafe_allow_html=True)


if st.button("刷新表单"):
  st.toast("表单刷新成功！", icon="ℹ️")