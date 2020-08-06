package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "io/ioutil"
    "net/http"
    "os"
    "strings"
    "time"

    log "github.com/sirupsen/logrus"
    "github.com/tidwall/sjson"
)

func getClusterHealth(sAddr string, tAddr string) {

    endPoint := "/_cluster/health"

    s := strings.TrimSpace("http://" + sAddr + endPoint)

    resp, err := http.Get(s)
    check(err,"getClusterHealth")

    defer resp.Body.Close()

    if r:= checkResCode(resp) ; r != true {
        return
    }

    byteData, err := ioutil.ReadAll(resp.Body)
    check(err,"getClusterHealth")

    var m map[string]interface{}

    err = json.Unmarshal(byteData, &m)
    check(err,"getClusterHealth")

    m["@timestamp"] = time.Now().UTC()
    newData, err := json.Marshal(m)
    check(err,"getClusterHealth")

    sendData(tAddr, newData)

}

func getClusterStat(sAddr string, tAddr string) {
    p := "/_cluster/stats"

    s := strings.TrimSpace("http://" + sAddr + p)

    resp, err := http.Get(s)
    check(err,"getClusterStat")

    defer resp.Body.Close()

    if r:= checkResCode(resp) ; r != true {
        return
    }

    byteData, err := ioutil.ReadAll(resp.Body)
    check(err,"getClusterStat")

    var m map[string]interface{}
    err = json.Unmarshal(byteData, &m)
    check(err,"getClusterStat")

    m["@timestamp"] = time.Now().UTC()
    newData, err := json.Marshal(m)
    check(err,"getClusterStat")

    sendData(tAddr, newData)
}

func getNodeStat(sAddr string, tAddr string) {

    p := "/_nodes/stats"

    s := strings.TrimSpace("http://" + sAddr + p)

    resp, err := http.Get(s)
    check(err,"getNodeStat")

    defer resp.Body.Close()

    if r:= checkResCode(resp) ; r != true {
        return
    }

    var m map[string]interface{}
    byteData, err := ioutil.ReadAll(resp.Body)
    err = json.Unmarshal(byteData, &m)
    check(err,"getNodeStat")

    m["@timestamp"] = time.Now()
    newData, err := json.Marshal(m)
    check(err,"getNodeStat")

    sendData(tAddr, newData)
}

func getIndexStat(sAddr string, tAddr string) {
    p := "/_stats"

    s := strings.TrimSpace("http://" + sAddr + p)

    resp, err := http.Get(s)
    check(err,"getIndexStat")

    defer resp.Body.Close()

    if r:= checkResCode(resp) ; r != true {
        return
    }

    byteData, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Println(err.Error())
    }

    var m map[string]interface{}

    err = json.Unmarshal(byteData, &m)
    check(err,"getIndexStat")

    //mod fields --- _all --- _shards --- can't stay here
    m["@timestamp"] = time.Now().UTC()
    m["all"] = m["_all"]
    m["_all"] = nil

    m["shards"] = m["_shards"]
    m["_shards"] = nil

    newData, err := json.Marshal(&m)
    check(err,"getIndexStat")

    data,err := sjson.Delete(string(newData),"_shards")
    check(err,"getIndexStat")

    data,err = sjson.Delete(string(newData),"_all")
    check(err,"getIndexStat")


    sendData(tAddr,[]byte(data))
}

func checkResCode(resp *http.Response) bool {

    if resp.StatusCode == 200 || resp.StatusCode == 201 {
        return true
    }

    log.Warningln(resp.Status)
    s,_ :=ioutil.ReadAll(resp.Body)
    log.Warningln(string(s))

    return false

}


func check(err error,funcname string) {

    //get caller name
    //pc, _, _, _ := runtime.Caller(1)
    //fmt.Println(runtime.FuncForPC(pc).Name())

    if err != nil {
        log.Fatal("Func ",funcname," - ",err.Error())
    }

}


func sendData(addr string, data []byte) {

    indexName := "es_data"
    typeName := "monitor"
    u := "http://" + addr + "/" + indexName + "/" + typeName + "/" + time.Now().UTC().String()

    url := strings.TrimSpace(u)

    resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
    check(err,"sendData")

    defer resp.Body.Close()

    if r:= checkResCode(resp) ; r != true {
        return
    }

    log.Println(resp.Status)

}


func init(){

    log.SetOutput(os.Stdout)

}

func main() {

    addrSource := "127.0.0.1:9200"
    addrEnd := "127.0.0.1:9200"

    flag.StringVar(&addrSource, "s", addrSource, "data source")
    flag.StringVar(&addrEnd, "t", addrEnd, "data backend")
    flag.Parse()


   for {
       getClusterHealth(addrSource, addrEnd)
       getClusterStat(addrSource, addrEnd)
       getNodeStat(addrSource, addrEnd)
       getIndexStat(addrSource,addrEnd)
       time.Sleep(1 * time.Second)
   }

}