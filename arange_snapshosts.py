#!/bin/python

import subprocess
import sys
import io
import os
import json

to_resolve_path = sys.argv[1]
dest_repo_path  = sys.argv[2]

##load all snapshosts to merge
#list snapshots in to_resolve
p = subprocess.Popen(['hdfs', 'dfs', '-ls', to_resolve_path], stdout=subprocess.PIPE) #, shell=True
(output, err) = p.communicate()
snapshots_data = list()
indices_data = dict()
for line in output.splitlines():
  for entry in str(line).split(" "):
    if to_resolve_path in entry:
      entry = entry.rstrip('\'')
      snapshot_index0_content = subprocess.Popen(['hdfs', 'dfs', '-cat', entry + "/index-0" ], stdout=subprocess.PIPE)
      (output2, err2) = snapshot_index0_content.communicate()
      output2_json = json.loads(output2)
      snapshots_data.extend(output2_json["snapshots"])
      indices_data = { **output2_json["indices"], **indices_data }

      #move indices and *.dat files
      res1 = subprocess.Popen(['hdfs', 'dfs', '-ls', entry + "/indices/"], stdout=subprocess.PIPE) #, shell=True
      (output3, err) = res1.communicate()

      #Here we get the main shard path and .../1, .../2, ... shards
      main_shard_str, *other_shards = sorted([x.rstrip('\'') for line2 in output3.splitlines() for x in str(line2).split(" ") if (to_resolve_path in x)], reverse=True)
      #Get snap-.....dat name in main shard
      res1 = subprocess.Popen(['hdfs', 'dfs', '-ls', main_shard_str + "/0/"], stdout=subprocess.PIPE) #, shell=True
      (output3, err) = res1.communicate()
      snap_path, *not_important = [x.rstrip('\'') for line2 in output3.splitlines() for x in str(line2).split(" ") if ("/0/snap-" in x)]
      snap_name = snap_path.split("/")[-1]

      #Copy main shard
      c = subprocess.Popen(['hdfs', 'dfs', '-cp', main_shard_str, dest_repo_path + "/indices/" ], stdout=subprocess.PIPE)
      (output, err) = c.communicate()
      c = subprocess.Popen(['hdfs', 'dfs', '-cp', entry + "/*.dat", dest_repo_path + "/" ], stdout=subprocess.PIPE)
      (output, err) = c.communicate()
      for other_shard in other_shards:
        #Copy other shards and rename snap-...dat files
        dest_dir = dest_repo_path + "/indices/" + main_shard_str.split("/")[-1] + "/"
        c = subprocess.Popen(['hdfs', 'dfs', '-cp', other_shard, dest_dir], stdout=subprocess.PIPE)
        (output, err) = c.communicate()
        c = subprocess.Popen(['hdfs', 'dfs', '-mv', dest_dir + other_shard.split("/")[-1] + "/snap-*", dest_dir + other_shard.split("/")[-1] + "/" + snap_name], stdout=subprocess.PIPE)
        (output, err) = c.communicate()
        
     
      #c = subprocess.Popen(['hdfs', 'dfs', '-rm', '-skipTrash', '-r', '-f', entry ], stdout=subprocess.PIPE)
      #(output, err) = c.communicate()

if 0 == len(snapshots_data):
  print("Nothing to do")
  sys.exit(0)

##load existing merged snapshots
#get current index.latest 
l = subprocess.Popen(['hdfs', 'dfs', '-ls',  dest_repo_path], stdout=subprocess.PIPE)
(output, err) = l.communicate()
latest_index_n = int(0) 
curr_index_n = None
for line in output.splitlines():
  for entry in str(line).split(" "):
    if dest_repo_path + "/index.latest" in entry: 
      c = subprocess.Popen(['hdfs', 'dfs', '-get', dest_repo_path + "/index.latest"], stdout=subprocess.PIPE)
      (output, err) = c.communicate()

      with open("index.latest", "rb") as content_file:
        content = content_file.read()
        curr_index_n = int.from_bytes(content, byteorder='big', signed=False)
        latest_index_n = curr_index_n + 1

      #Remove file after use
      os.remove("index.latest")

#store new latest index value to file as byte array
index_n_ba = latest_index_n.to_bytes(8, byteorder='big', signed=False)
with open("index.latest.new", "wb") as fh:
  fh.write(index_n_ba)

#load existing index-n file 
if curr_index_n is not None:
  curr_indexN_content = subprocess.Popen(['hdfs', 'dfs', '-cat', dest_repo_path + "/index-{}".format(str(curr_index_n)) ], stdout=subprocess.PIPE)
  (output, err) = curr_indexN_content.communicate()
  output_json = json.loads(output)
  snapshots_data.extend(output_json["snapshots"])
  indices_data = { **output_json["indices"], **indices_data }

##copy snapshot files

##generate merged index-(n)
#Here we create a json that describes the snapshots in to_resolve
index_n = { 'snapshots': snapshots_data, 'indices': indices_data }
with open("index-{}".format(str(latest_index_n)), "w") as fh:
  fh.write(json.dumps(index_n))

#upload new index-n file to cluster
c = subprocess.Popen(['hdfs', 'dfs', '-put', '-f', "index-{}".format(str(latest_index_n)),  dest_repo_path + "/index-{}".format(str(latest_index_n))], stdout=subprocess.PIPE)
(output, err) = c.communicate()

#upload new index.latest to cluster
c = subprocess.Popen(['hdfs', 'dfs', '-put', '-f', "index.latest.new",  dest_repo_path + "/index.latest"], stdout=subprocess.PIPE)
(output, err) = c.communicate()
