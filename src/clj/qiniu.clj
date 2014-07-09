(ns clj.qiniu
  (:import [com.qiniu.api.config Config]
           [com.qiniu.api.rs PutPolicy URLUtils GetPolicy RSClient Entry
            EntryPath EntryPathPair BatchCallRet BatchStatRet]
           [java.io InputStream File]
           [com.qiniu.api.io IoApi PutExtra PutRet]
           [com.qiniu.api.net CallRet]
           [com.qiniu.api.fop ImageInfo ImageInfoRet
            ImageExif ExifRet ExifValueType
            ImageView]
           [com.qiniu.api.auth.digest Mac]
           [com.qiniu.api.rsf RSFClient ListPrefixRet ListItem])
  (:require [clojure.java.io :as io]))

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
         (initialValue [] true)
         (deref [] (.get ~(with-meta 'this {:tag `ThreadLocal})))))))

(defonce throw-exception? (atom false))

(defn set-config!
  "Set global config for qiniu sdk."
  [& {:keys [access-key secret-key user-agent throw-exception?] :or {user-agent "Clojure qiniu sdk 0.1"}}]
  (do
    (set-value! Config/ACCESS_KEY access-key)
    (set-value! Config/SECRET_KEY secret-key)
    (set-value! Config/USER_AGENT user-agent)
    (when throw-exception?
      (reset! throw-exception? throw-exception?))
    Config))

(defn- ^Mac create-mac [& {:keys [access-key secret-key]}]
  (Mac. (or access-key Config/ACCESS_KEY) (or secret-key Config/SECRET_KEY)))

(defn uptoken
  "Create a uptoken for uploading file. see http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#make-uptoken"
  [bucket & {:keys [access-key secret-key expires scope callbackUrl asyncOps returnBody escape detectMime insertOnly mimeLimit persistentOps persistentPipeline persistentNotifyUrl saveKey endUser fsizeLimit] :as opts}]
  (let [mac (apply create-mac opts)
        ^PutPolicy pp (PutPolicy. bucket)]
    (set-value! (.expires pp) expires)
    (set-value! (.scope pp) scope)
    (set-value! (.callbackUrl pp) callbackUrl)
    (set-value! (.asyncOps pp) asyncOps)
    (set-value! (.returnBody pp) returnBody)
    (set-value! (.escape pp) escape)
    (set-value! (.detectMime pp) detectMime)
    (set-value! (.insertOnly pp) insertOnly)
    (set-value! (.mimeLimit pp) mimeLimit)
    (set-value! (.persistentOps pp) persistentOps)
    (set-value! (.persistentPipeline pp) persistentPipeline)
    (set-value! (.persistentNotifyUrl pp) persistentNotifyUrl)
    (set-value! (.saveKey pp) saveKey)
    (set-value! (.endUser pp) endUser)
    (set-value! (.fsizeLimit pp) fsizeLimit)
    (.token pp mac)))

(defn- callret->map [^CallRet ret]
  (when ret
    (let [m {:status (.getStatusCode ret) :response (.getResponse ret)}]
      (when (.ok ret)
        m
        (if @throw-exception?
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
  "Upload a file to qiniu storage by token and key."
  [^String token ^String key file & opts]
  (let [^PutExtra extra (apply extra->instance opts)
        ^InputStream is (io/input-stream file)]
    (putret->map (IoApi/Put token key is extra))))

(defn upload-bucket
  "Upload a file to qiniu storage bucket."
  [bucket key file & opts]
  (apply upload (uptoken bucket) key file opts))

(defn public-download-url
  "Create a download url for public file."
  [domain key]
  (URLUtils/makeBaseUrl domain key))

(defn private-download-url
  "Create a download url for public file."
  [domain key & {:keys [expires access-key secret-key] :as opts}]
  (let [mac (apply create-mac opts)
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
  (conj batch-entries v))

(defn- entry->map [^Entry e]
  (merge (callret->map e)
         (when e
           {:fsize (.getFsize e)
            :hash (.getHash e)
            :mimeType (.getMimeType e)
            :putTime (.getPutTime e)})))

(defn- create-entry-path [bucket key]
  (let [^EntryPath ep (EntryPath.)]
    (set! (.bucket ep) bucket)
    (set! (.key ep) key)
    ep))

(defn- ^RSClient rs-client [ & opts]
  (RSClient. (apply create-mac opts)))

(defn stat
  "Stat a file."
  [bucket key & opts]
  (if-not batch-mode
    (->
     (apply rs-client opts)
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
     (apply rs-client opts)
     (.copy src-bucket src-key dst-bucket dst-key)
     (callret->map))
    (add-batch-entry :copy
                     (create-entry-pair src-bucket src-key dst-bucket dst-key))))

(defn move
  "Move a file"
  [src-bucket src-key dst-bucket dst-key & opts]
  (if-not batch-mode
    (->
     (apply rs-client opts)
     (.move src-bucket src-key dst-bucket dst-key)
     (callret->map))
    (add-batch-entry :move
                     (create-entry-pair src-bucket src-key dst-bucket dst-key))))

(defn Delete
  "Delete a file."
  [bucket key & opts]
  (if-not batch-mode
    (->
     (apply rs-client opts)
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
  (when ret
    (if (= op :stat)
      (map entry->map (.results ^BatchStatRet ret))
      (map callret->map (.results ^BatchCallRet ret)))))

(defn exec
  "Execute  batch operations.The entries must be the same type."
  [& {:keys [entries] :as opts}]
  (-> (apply rs-client opts)
      (exec-batch (or entries batch-entries) @batch-op)
      (convert-batchret @batch-op)))

(defmacro with-batch [ & body]
  `(binding [batch-entries (vector)]
     ~@body))

(defn image-info
  "Get the picture's basic information,such as format,width,height and colorModel.
  http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#fop-image-info"
  [url & opts]
  (when-let [^ImageInfoRet ret (ImageInfo/call url (apply create-mac opts))]
    {:format (.format ret)
     :width (.width ret)
     :height (.height ret)
     :colorModel (.colorModel ret)}))

(defn image-exif
  "Get the image's exif."
  [url & opts]
  (when-let [^ExifRet ret (ImageExif/call url (apply create-mac opts))]
    (into {}
          (map (fn [[k ^ExifValueType v]]
                 [k (when v {:type (.type v) :value (.value v)})])
               (into {} (.result ret))))))

(defn image-view
  "Make a image thumbnail.
  http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#fop-image-view"
  [url & {:keys [mode width height quality format] :or {format "png" quality 100 mode 1} :as opts}]
  (let [^ImageView iv (ImageView.)]
    (set-value! (.mode iv) mode)
    (set-value! (.height iv) height)
    (set-value! (.width iv) width)
    (set-value! (.quality iv) quality)
    (set-value! (.format iv) format)
    (-> iv
        (.call url (apply create-mac opts))
        (callret->map))))

(defn- listitem-map [^ListItem it]
  (when it
    {:key (.key it)
     :hash (.hash it)
     :fsize (.fsize it)
     :putTime (.putTime it)
     :mimeType (.mimeType it)
     :endUser (.endUser it)}))

(defn list-files
  "List files in a bucket. Returns a lazy sequence of result files.
  http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#rsf-listPrefix"
  [bucket prefix & {:keys [limit marker rsf-client] :or {limit 32 marker ""} :as opts}]
  (let [rsf-client (or rsf-client (RSFClient. (apply create-mac opts)))]
    (when-let [^ListPrefixRet ret (->
                                   rsf-client
                                   (.listPrifix bucket prefix marker limit))]
      (let [^String marker (.marker ret)
            results (.results ret)]
        (concat
         (map listitem-map results)
         (lazy-seq (list-files bucket prefix :limit limit :rsf-client rsf-client :marker marker)))))))
