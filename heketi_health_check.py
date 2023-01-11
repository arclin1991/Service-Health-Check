#!/usr/bin/env python3
# encoding: utf-8


from kubernetes import client, config
from kubernetes.config import load_kube_config
import os
import yaml
import requests
import os
import time


load_kube_config()
v1 = client.CoreV1Api()

def get_heketi_api_status_fn():
    try:
        r = requests.get("http://heketi.kube-storage.svc.cluster.local.:8080/hello", verify=False)
        return(r)
    except requests.exceptions.ConnectionError:
        print("request Connection Error")

def create_pvc_fn(spec, name, namespace, **kwargs):

    pvc_list = get_pvc_fn()
    pvc_name_count = 0
    #############Check if the same pvc exists###################
    for uid_list in pvc_list:
        if pvc_list[uid_list]["name"] == name:
            pvc_name_count += 1
    
    if pvc_name_count == 0:
        size = spec

        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size,)
        data = yaml.safe_load(text)

        obj = v1.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=data,
        )
        return ( "create " + name + " pvc success")
    else: 
        return ( name + "already exists")

def delete_pvc_fn(name, namespace):
    pvc_list = get_pvc_fn()
    pvc_name_count = 0
    pvc_status_list = []
    pvc_pending_count = 0

    #############Check if the same pvc exists###################
    for uid_list in pvc_list:
        if pvc_list[uid_list]["name"] == name:
            pvc_status_list.append(pvc_list[uid_list]["phase"])
            pvc_name_count += 1

    if pvc_name_count > 0:
        time.sleep( 10 )
        pvc_list_new = get_pvc_fn()
        for pvc_status in pvc_list_new:
            #print(pvc_list_new[pvc_status]["phase"])
            #print(pvc_list_new[pvc_status]["name"])
            if pvc_list_new[pvc_status]["name"] == name and pvc_list_new[pvc_status]["phase"] == "Bound":
                print(pvc_list_new[pvc_status]["phase"])
                if pvc_list_new[pvc_status]["phase"] == "Bound":
                    v1.delete_namespaced_persistent_volume_claim(name, namespace, async_req=True)
                else:
                    print(pvc_pending_count)
                    #print (pvc_pending_count)
                    return ( "Please check gfs-user storage class")
        return ( "Delete " + name + " pvc success")
    else: 
        return ("Not delete because " + name + " does not exist")

def delete_pv_fn(name, spec):
    #pvcs = {}
    size = spec
    thread = v1.list_persistent_volume_with_http_info(async_req=True)
    result = thread.get()
    pv_name_count = 0
    pv_sum = (len(result[0].items))
    uid_name_list = []
    #print (result[0].items[1].spec.claim_ref.name)
    #print (result[0].items[0].spec.claim_ref.uid)
    for i in range(0,pv_sum):
        #print (result[0].items[i].spec.claim_ref.name)
        if (result[0].items[i].spec.claim_ref.name) == name:  
            uid_name_list.append(result[0].items[i].spec.claim_ref.uid)    
            pv_name_count += 1
    #print(uid_name)
    if pv_name_count > 0:
        path = os.path.join(os.path.dirname(__file__), 'pvc.yaml')
        tmpl = open(path, 'rt').read()
        text = tmpl.format(name=name, size=size,)
        data = yaml.safe_load(text)
        for uid_name in uid_name_list:
            uid="pvc-" + uid_name
            v1.patch_persistent_volume_with_http_info(uid, {"spec":{"persistentVolumeReclaimPolicy":"Delete"}}, async_req=True)
        return ( "Delete " + name + " pv success")
    else: 
        return ("Not delete because " + name + " does not exist")



def get_pvc_fn():
    pvcs = {}
    ret = v1.list_persistent_volume_claim_for_all_namespaces(watch=False)
    for i in ret.items:
        if i.spec.storage_class_name == 'gfs-user':
            pvcs[i.metadata.uid] = {"name": i.metadata.name, "namespace":
                i.metadata.namespace, "storageclass": i.spec.storage_class_name,
                "labels": i.metadata.labels, "phase": i.status.phase}
    return pvcs
    
if __name__=="__main__":
    print (get_heketi_api_status_fn())
    print (create_pvc_fn('1Gi', "arc-pvc", "kube-storage" ))
    #print (print(get_pvc_fn()))
    print (delete_pvc_fn("arc-pvc", "kube-storage" ))
    print (delete_pv_fn("arc-pvc","1Gi"))
    