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
    check(err)

    defer resp.Body.Close()

    byteData, err := ioutil.ReadAll(resp.Body)
    check(err)

    var m map[string]interface{}

    err = json.Unmarshal(byteData, &m)
    check(err)

    m["@timestamp"] = time.Now().UTC()
    newData, err := json.Marshal(m)
    check(err)

    sendData(tAddr, newData)

}

func getClusterStat(sAddr string, tAddr string) {
    p := "/_cluster/stats"

    s := strings.TrimSpace("http://" + sAddr + p)


    resp, err := http.Get(s)
    check(err)

    defer resp.Body.Close()

    byteData, err := ioutil.ReadAll(resp.Body)
    check(err)

    var m map[string]interface{}
    err = json.Unmarshal(byteData, &m)
    check(err)

    m["@timestamp"] = time.Now().UTC()
    newData, err := json.Marshal(m)
    check(err)

    log.Println(string(newData))

    sendData(tAddr, newData)
}

func getNodeStat(sAddr string, tAddr string) {

    p := "/_nodes/stats"

    s := strings.TrimSpace("http://" + sAddr + p)

    resp, err := http.Get(s)
    check(err)

    defer resp.Body.Close()

    bytedata, err := ioutil.ReadAll(resp.Body)
    check(err)

    var m map[string]interface{}
    err = json.Unmarshal(bytedata, &m)
    check(err)

    m["@timestamp"] = time.Now()
    newData, err := json.Marshal(m)
    check(err)

    sendData(tAddr, newData)
}

func getIndexStat(sAddr string, tAddr string) {
    p := "/_stats"

    s := strings.TrimSpace("http://" + sAddr + p)

    resp, err := http.Get(s)
    check(err)

    defer resp.Body.Close()

    byteData, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Println(err.Error())
    }

    var m map[string]interface{}

    err = json.Unmarshal(byteData, &m)
    check(err)

    //mod fields --- _all can't stay here
    m["@timestamp"] = time.Now().UTC()
    m["all"] = m["_all"]
    m["_all"] = nil

    newData, err := json.Marshal(&m)
    check(err)

    data,err := sjson.Delete(string(newData),"_all")
    check(err)

    sendData(tAddr,[]byte(data))
}

func check(err error) {

    //get caller name
    //pc, _, _, _ := runtime.Caller(1)
    //fmt.Println(runtime.FuncForPC(pc).Name())

    if err != nil {
        log.WithError(err)
    }

}


func sendData(addr string, data []byte) {

    indexName := "es-data"
    typeName := "monitor"
    u := "http://" + addr + "/" + indexName + "/" + typeName + "/" + time.Now().UTC().String()

    url := strings.TrimSpace(u)

    resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
    check(err)

    defer resp.Body.Close()

    log.Println(resp.Status)
    s,_ :=ioutil.ReadAll(resp.Body)
    log.Println(string(s))


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
        time.Sleep(1 + time.Second)
    }

}