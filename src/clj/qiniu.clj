(ns clj.qiniu
  "Clojure sdk for qiniu storage."
  {:author "dennis zhuang"
   :email "killme2008@gmail.com"
   :home "https://github.com/killme2008/clj.qiniu"}
  (:import [com.qiniu.api.config Config]
           [com.qiniu.api.rs PutPolicy URLUtils GetPolicy RSClient Entry
            EntryPath EntryPathPair BatchCallRet BatchStatRet]
           [java.io InputStream File]
           [org.apache.commons.codec.binary Base64]
           [com.qiniu.api.io IoApi PutExtra PutRet]
           [com.qiniu.api.net CallRet]
           [com.qiniu.api.fop ImageInfo ImageInfoRet
            ImageExif ExifRet ExifValueType
            ImageView]
           [com.qiniu.api.auth.digest Mac]
           [com.qiniu.api.rsf RSFClient ListPrefixRet ListItem RSFEofException])
  (:require [clojure.java.io :as io]
            [clj-http.client :as http]))

(defmacro ^:private set-value! [k v]
  `(when ~v
     (set! ~k ~v)))

(defmacro def-threadlocal-var
  "A macro to define thread-local var.
    It also implement clojure.lang.IDeref interface,
   so you can get it's value by @ or deref."
  [name]
  (let [name (with-meta name {:tag ThreadLocal})]
    `(def ~name
       (proxy [ThreadLocal clojure.lang.IDeref] []
         (initialValue [] nil)
         (deref [] (.get ~(with-meta 'this {:tag `ThreadLocal})))))))

(defonce ^:private throw-exception-atom? (atom false))

(defn set-config!
  "Set global config for qiniu sdk."
  [& {:keys [access-key secret-key user-agent throw-exception?] :or {user-agent "Clojure/qiniu sdk"}}]
  (do
    (set-value! Config/ACCESS_KEY access-key)
    (set-value! Config/SECRET_KEY secret-key)
    (set-value! Config/USER_AGENT user-agent)
    (when throw-exception?
      (reset! throw-exception-atom? throw-exception?))
    Config))

(defn- ^Mac create-mac [{:keys [access-key secret-key]}]
  (Mac. (or access-key Config/ACCESS_KEY) (or secret-key Config/SECRET_KEY)))

(defn uptoken
  "Create a uptoken for uploading file. see http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#make-uptoken"
  [bucket & {:keys [access-key secret-key expires scope callbackUrl callbackBody asyncOps returnUrl returnBody detectMime insertOnly mimeLimit persistentOps persistentNotifyUrl saveKey endUser fsizeLimit] :as opts}]
  (let [mac (create-mac opts)
        ^PutPolicy pp (PutPolicy. bucket)]
    (set-value! (.expires pp) expires)
    (set-value! (.scope pp) scope)
    (set-value! (.callbackUrl pp) callbackUrl)
    (set-value! (.callbackBody pp) callbackBody)
    (set-value! (.asyncOps pp) asyncOps)
    (set-value! (.returnUrl pp) returnUrl)
    (set-value! (.returnBody pp) returnBody)
    (set-value! (.detectMime pp) detectMime)
    (set-value! (.insertOnly pp) insertOnly)
    (set-value! (.mimeLimit pp) mimeLimit)
    (set-value! (.persistentOps pp) persistentOps)
    (set-value! (.persistentNotifyUrl pp) persistentNotifyUrl)
    (set-value! (.saveKey pp) saveKey)
    (set-value! (.endUser pp) endUser)
    (set-value! (.fsizeLimit pp) fsizeLimit)
    (.token pp mac)))

(defn- callret->map [^CallRet ret]
  (when ret
    (let [m {:status (.getStatusCode ret) :response (.getResponse ret)}]
      (if (.ok ret)
        (assoc m :ok true)
        (if @throw-exception-atom?
          (throw (ex-info "Server return error." m))
          (assoc m :exception (.getException ret)))))))

(defn- putret->map
  "Convert the PutRet to a hash map."
  [^PutRet ret]
  (merge (callret->map ret)
         (when ret
           {:hash (.getHash ret)
            :key (.getKey ret)})))


(defn- stringify-map-keys [m]
  (when m
    (into {}
          (map (fn [[k v]]
                 [(name k)
                  (if (map? v)
                    (stringify-map-keys v)
                    v)]) m))))

(defonce auto-crc32 IoApi/AUTO_CRC32)
(defonce no-crc32 IoApi/NO_CRC32)
(defonce with-crc32 IoApi/WITH_CRC32)

(defn- ^PutExtra extra->instance [& {:keys [checkCrc crc32 mimeType params]}]
  (let [^PutExtra extra (PutExtra.)]
    (set-value! (.checkCrc extra) checkCrc)
    (set-value! (.crc32 extra) crc32)
    (set-value! (.mimeType extra) mimeType)
    (set-value! (.params extra) (stringify-map-keys params))
    extra))

(defn upload
  "Upload a file to qiniu storage by token and key.
  The file should can be convert into InputStream by
     clojure.java.io/input-stream
  function."
  [^String token ^String key file & opts]
  (let [^PutExtra extra (apply extra->instance opts)
        ^InputStream is (io/input-stream file)]
    (putret->map (IoApi/Put token key is extra))))

(defn upload-bucket
  "Upload a file to qiniu storage bucket.
  The file should can be convert into InputStream by
     clojure.java.io/input-stream
  function.
  "
  [bucket key file & opts]
  (apply upload (uptoken bucket) key file opts))

(defn public-download-url
  "Create a download url for public file."
  [domain key]
  (URLUtils/makeBaseUrl domain key))

(defn private-download-url
  "Create a download url for public file."
  [domain key & {:keys [expires access-key secret-key] :as opts}]
  (let [mac (create-mac opts)
        ^String base-url (public-download-url domain key)
        ^GetPolicy gp (GetPolicy.)]
    (set-value! (.expires gp) expires)
    (.makeRequest gp base-url mac)))

(def ^{:private true :dynamic true} batch-mode false)
(def ^{:private true :dynamic true} batch-entries nil)
(def-threadlocal-var batch-op)

(defn- set-batch-op [op]
  (if-let [exists @batch-op]
    (when-not (= exists op)
      (throw (ex-info (str "Already in batch " (name exists) " mode.")
                      {:op exists})))
    (.set batch-op op)))

(defn- add-batch-entry [op v]
  (set-batch-op op)
  (swap! batch-entries conj v))

(defn- entry->map [^Entry e]
  (merge (callret->map e)
         (when (and e (.ok e))
           {:fsize (.getFsize e)
            :hash (.getHash e)
            :mimeType (.getMimeType e)
            :putTime (.getPutTime e)})))

(defn- create-entry-path [bucket key]
  (let [^EntryPath ep (EntryPath.)]
    (set! (.bucket ep) bucket)
    (set! (.key ep) key)
    ep))

(defn- ^RSClient rs-client [opts]
  (RSClient. (create-mac opts)))

(defn stat
  "Stat a file."
  [bucket key & opts]
  (if-not batch-mode
    (->
     (rs-client (apply hash-map opts))
     (.stat bucket key)
     (entry->map))
    (add-batch-entry :stat (create-entry-path bucket key))))

(defn- ^EntryPathPair create-entry-pair [sb sk db dk]
  (let [^EntryPathPair pair (EntryPathPair.)
        ^EntryPath src (create-entry-path sb sk)
        ^EntryPath dst (create-entry-path db dk)]
    (set! (.src pair) src)
    (set! (.dest pair) dst)
    pair))

(defn copy
  "Copy a file"
  [src-bucket src-key dst-bucket dst-key & opts]
  (if-not batch-mode
    (->
     (rs-client (apply hash-map opts))
     (.copy src-bucket src-key dst-bucket dst-key)
     (callret->map))
    (add-batch-entry :copy
                     (create-entry-pair src-bucket src-key dst-bucket dst-key))))

(defn move
  "Move a file"
  [src-bucket src-key dst-bucket dst-key & opts]
  (if-not batch-mode
    (->
     (rs-client (apply hash-map opts))
     (.move src-bucket src-key dst-bucket dst-key)
     (callret->map))
    (add-batch-entry :move
                     (create-entry-pair src-bucket src-key dst-bucket dst-key))))

(defn delete
  "Delete a file."
  [bucket key & opts]
  (if-not batch-mode
    (->
     (rs-client (apply hash-map opts))
     (.delete bucket key)
     (callret->map))
    (add-batch-entry :delete (create-entry-path bucket key))))

(defmulti exec-batch
  "excute batch operations based on op type."
  {:private true}
  (fn [rs-client entries op] op))

(defmethod exec-batch :stat [^RSClient rc entries _]
  (->
   rc
   (.batchStat entries)))

(defmethod exec-batch :copy [^RSClient rc entries _]
  (->
   rc
   (.batchCopy entries)))

(defmethod exec-batch :move [^RSClient rc entries _]
  (->
   rc
   (.batchMove entries)))

(defmethod exec-batch :delete [^RSClient rc entries _]
  (->
   rc
   (.batchDelete entries)))

(defmethod exec-batch nil [_ _ _]
  nil)

(defmethod exec-batch :default [_ _ op]
  (throw (ex-info (str "Unknown op:" op) {:op op})))

(defn- convert-batchret [ret op]
  (merge (callret->map ret)
         (when (and ret (.ok ret))
           {:results
            (if (= op :stat)
              (map entry->map (.results ^BatchStatRet ret))
              (map callret->map (.results ^BatchCallRet ret)))})))

(defn exec
  "Execute  batch operations.The entries must be the same type."
  [& {:keys [entries] :as opts}]
  (if batch-mode
    (try
      (-> (rs-client opts)
          (exec-batch (or entries @batch-entries) @batch-op)
          (convert-batchret @batch-op))
      (finally
        (reset! batch-entries [])
        (.remove batch-op)))
    (throw (ex-info "exec must be invoked in with-batch body."))))

(defmacro with-batch [ & body]
  `(binding [batch-entries (atom [])
             batch-mode true]
     (try
       ~@body
       (finally
         (.remove batch-op)))))

(defn image-info
  "Get the picture's basic information,such as format,width,height and colorModel.
  http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#fop-image-info"
  [url & opts]
  (when-let [^ImageInfoRet ret (ImageInfo/call url (create-mac (apply hash-map opts)))]
    {:format (.format ret)
     :width (.width ret)
     :height (.height ret)
     :colorModel (.colorModel ret)}))

(defn image-exif
  "Get the image's exif."
  [url & opts]
  (when-let [^ExifRet ret (ImageExif/call url (create-mac (apply hash-map opts)))]
    (merge (callret->map ret)
           (when (and ret (.ok ret))
             {:exif
              (into {}
                    (map (fn [[k ^ExifValueType v]]
                           [k (when v {:type (.type v) :value (.value v)})])
                         (into {} (.result ret))))}))))

(defn image-view
  "Make a image thumbnail.
  http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#fop-image-view"
  [url & {:keys [mode width height quality format] :or {format "png" quality 80 width 100 height 100 mode 1} :as opts}]
  (let [^ImageView iv (ImageView.)]
    (set-value! (.mode iv) mode)
    (set-value! (.height iv) height)
    (set-value! (.width iv) width)
    (set-value! (.quality iv) quality)
    (set-value! (.format iv) format)
    (-> iv
        (.call url (create-mac opts))
        (callret->map))))

(defn- listitem->map [^ListItem it]
  (when it
    {:key (.key it)
     :hash (.hash it)
     :fsize (.fsize it)
     :putTime (.putTime it)
     :mimeType (.mimeType it)
     :endUser (.endUser it)}))

(defn bucket-file-seq
  "List files in a bucket. Returns a lazy sequence of result files."
  [bucket prefix & {:keys [limit marker rsf-client] :or {limit 32 marker ""} :as opts}]
  (let [rsf-client (or rsf-client (RSFClient. (create-mac opts)))]
    (when-let [^ListPrefixRet ret (->
                                   rsf-client
                                   (.listPrifix bucket prefix marker limit))]
      (let [^String marker (.marker ret)
            results (.results ret)]
        (concat
         (map listitem->map results)
         (if (.ok ret)
           (lazy-seq (bucket-file-seq bucket prefix :limit limit :rsf-client rsf-client :marker marker))
           (when-not (instance? RSFEofException (.exception ret))
             (throw (ex-info "List bucket files fails." {:exception (.exception ret)})))))))))

(defonce valid-stat-items #{"apicall" "transfer" "space"})
(defonce qiniu-api-url "http://api.qiniu.com")
(def stats-grain "day")
(defn- make-authorization [^String path]
  (let [^Mac mac (create-mac nil)]
    (str "QBox " (.sign mac (.getBytes path)))))

(defn- http-request [path f & {:keys [method domain] :or {method :get domain qiniu-api-url}}]
  (let [{:keys [status body]} (http/request
                               {:socket-timeout 10000
                                :conn-timeout 5000
                                :method method
                                :url (str domain path)
                                :throw-exceptions false
                                :as :json
                                :client-params
                                {"http.useragent" Config/USER_AGENT}
                                :headers
                                {"Authorization"
                                 (make-authorization
                                  (str path "\n"))}})]
    (if (= status 200)
      {:ok true
       :results (f body)}
      (if @throw-exception-atom?
        (throw (ex-info "Requst failed." {:body body}))
        {:ok false :response body :status status}))))

(defn bucket-stats
  "Get the bucket statistics info."
  [bucket item from to & opts]
  (when-not (valid-stat-items item)
    (throw (ex-info "Invalid stats item" {:items valid-stat-items})))
  (let [path (str "/stat/select/" item "?bucket=" bucket "&from=" from "&to=" to "&p=" stats-grain)
        path (if (seq opts)
               (str path "&"
                    (clojure.string/join "&"
                                         (map #(clojure.string/join "=" %)
                                              (apply hash-map (map clojure.core/name opts)))))
               path)]
    (http-request path (fn [body]
                         (zipmap (:time body)
                                 (:data body))))))


(defn bucket-monthly-stats
  "Get the bucket statstics info by month."
  [bucket month]
  (let [path (str "/stat/info?bucket=" bucket "&month=" month)]
    (http-request path identity)))


;;bucket management
(defonce rs-api-domain "http://rs.qiniu.com")

(defn mk-bucket
  "Create a bucket"
  [bucket]
  (http-request  (str "/mkbucket/" bucket) identity
                 :method :post
                 :domain rs-api-domain))

(defn remove-bucket
  "Delete a bucket"
  [bucket]
  (http-request (str "/drop/" bucket) identity
                :method :post
                :domain rs-api-domain))

(defn- encode-base64ex
  [src]
  (let [b64 (Base64/encodeBase64 src)
        length (alength ^bytes b64)]
    (doseq [i (range 0 length)]
      (do
        (if (= 47 (aget b64 i))
          (aset-byte b64 i 95))
        (if (= 43 (aget b64 i))
          (aset-byte b64 i 45))))
    b64))

(defn url-safe-encode-bytes
  [^bytes src]
  (let [length (alength src)]
    (if (zero? (rem (alength src) 3))
      (encode-base64ex src)
      (let [^bytes src (encode-base64ex src)
            length (alength src)
            remainder (rem length 4)]
        (if (zero? remainder)
          src
          (let [pad (- 4 remainder)
                padded-bytes (byte-array (+ length pad))]
            (System/arraycopy src 0 padded-bytes 0 length)
            (aset-byte padded-bytes length 61)
            (if (> pad 1)
              (aset-byte padded-bytes (inc length) 61))
            padded-bytes))))))


(defn publish-bucket
  "Publish bucket as public domain."
  [bucket domain]
  (http-request (str "/publish/" (String. ^bytes (url-safe-encode-bytes (.getBytes ^String domain)))  "/from/" bucket) identity
                :method :post
                :domain rs-api-domain))

(defn list-buckets
  "List all buckets"
  []
  (http-request "/buckets" identity
                :method :post
                :domain rs-api-domain))

(defn pfop
  "Trigger fops for an exists resource in bucket.Returns a persistentId.
   see http://developer.qiniu.com/docs/v6/api/reference/fop/pfop/pfop.html"
   [bucket key fops notifyURL & opts]
  (let [path (format "/pfop/?bucket=%s&key=%s&fops=%s" bucket key fops)
        path (if (seq opts)
               (str path "&"
                    (clojure.string/join "&"
                                         (map #(clojure.string/join "=" %)
                                              (apply hash-map (map clojure.core/name opts)))))
               path)]
    (http-request path (fn [body]
                         (:persistentId body))
                  :method :post)))

(defn prefop-status
  "Retrieve pfop status.
   See http://developer.qiniu.com/docs/v6/api/reference/fop/pfop/prefop.html"
  [id]
  (http-request (str "/status/get/prefop?id=" id) identity))
