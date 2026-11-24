package main
import ("compress/gzip";"flag";"fmt";"net/http";"strings";"strconv";"io";"time")
func main(){
 port:=flag.String("port","9101","");flag.Parse()
 text:=[]byte(strings.Repeat(`{"id":12345,"name":"deploy","status":"running","logs":"lorem ipsum dolor sit amet"}`+"\n",20000)) // ~1.7MB compressible
 mux:=http.NewServeMux()
 mux.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){w.Write([]byte("ok"))})
 mux.HandleFunc("/small",func(w http.ResponseWriter,r *http.Request){w.Write([]byte(strings.Repeat("x",2000)))})
 mux.HandleFunc("/download",func(w http.ResponseWriter,r *http.Request){
  mb,_:=strconv.Atoi(r.URL.Query().Get("mb")); if mb==0{mb=10}
  w.Header().Set("Content-Type","application/octet-stream")
  w.Header().Set("Content-Length",strconv.Itoa(mb<<20))
  buf:=make([]byte,64<<10); for i:=range buf{buf[i]=byte(i*7+i>>8)}
  for n:=0;n<mb<<20;n+=len(buf){w.Write(buf)}
 })
 mux.HandleFunc("/json",func(w http.ResponseWriter,r *http.Request){
  w.Header().Set("Content-Type","application/json")
  if strings.Contains(r.Header.Get("Accept-Encoding"),"gzip"){
   w.Header().Set("Content-Encoding","gzip");g:=gzip.NewWriter(w);g.Write(text);g.Close();return}
  w.Write(text)})
 mux.HandleFunc("/slowdl",func(w http.ResponseWriter,r *http.Request){ // throttled client-like stream
  buf:=make([]byte,32<<10);for i:=0;i<32;i++{w.Write(buf);w.(http.Flusher).Flush();time.Sleep(20*time.Millisecond)}})
 _=fmt.Sprint;_=io.EOF
 http.ListenAndServe(":"+*port,mux)}
