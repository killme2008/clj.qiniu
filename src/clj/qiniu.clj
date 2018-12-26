(ns clj.qiniu
  "Clojure sdk for qiniu storage."
  {:author "dennis zhuang"
   :email "killme2008@gmail.com"
   :home "https://github.com/leancloud/clj.qiniu"}
  (:import [java.io InputStream File]
           [java.net URLEncoder]

           [com.google.gson Gson]
           [com.qiniu.util Auth StringMap Base64]
           [com.qiniu.http Response]
           [com.qiniu.common Zone QiniuException]
           [com.qiniu.storage Configuration BucketManager UploadManager BucketManager$BatchOperations]
           [com.qiniu.storage.model BatchStatus DefaultPutRet FileInfo BatchOpData])
  (:require [clojure.java.io :as io]
            [clj-http.client :as http]
            [clj-http.conn-mgr :refer [make-reusable-conn-manager]]))

(defmacro ^:private reset-value! [k v]
  `(when ~v
     (reset! ~k ~v)))

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
(defonce ^:private num-http-threads-atom (atom 10))
(defonce ^:private http-conn-manager
  (delay
   (make-reusable-conn-manager
    {:timeout 10 :threads @num-http-threads-atom :default-per-route @num-http-threads-atom})))
(defonce ^:private UP-HOST (atom "http://up.qiniu.com"))
(defonce ^:private ACCESS-KEY (atom ""))
(defonce ^:private SECRET-KEY (atom ""))
(defonce ^:private USER-AGENT (atom "Clojure/qiniu sdk 1.0"))

(defn set-config!
  "Set global config for qiniu sdk."
  [& {:keys [access-key secret-key user-agent throw-exception? num-http-threads
             up-host] :or {user-agent "Clojure/qiniu sdk 1.0"
                           up-host "http://up.qiniu.com"
                           num-http-threads 10}}]
  (do
    (reset-value! UP-HOST up-host)
    (reset-value! ACCESS-KEY access-key)
    (reset-value! SECRET-KEY secret-key)
    (reset-value! USER-AGENT user-agent)
    (reset-value! throw-exception-atom? throw-exception?)
    (reset-value! num-http-threads-atom num-http-threads))
  {:UP-HOST @UP-HOST
   :ACCESS-KEY @ACCESS-KEY
   :SECRET-KEY @SECRET-KEY
   :USER-AGENT @USER-AGENT})

(defn ^Auth create-auth [{:keys [access-key secret-key]}]
  (Auth/create (or access-key @ACCESS-KEY) (or secret-key @SECRET-KEY)))

(defn uptoken
  "Create a uptoken for uploading file. see http://developer.qiniu.com/docs/v6/sdk/java-sdk.html#make-uptoken"
  [bucket & {:keys [access-key secret-key expires mimeLimit persistentOps detectMime deadline endUser isPrefixalScope fsizeMin callbackBodyType returnBody saveKey callbackHost persistentPipeline callbackBody scope fileType persistentNotifyUrl fsizeLimit insertOnly returnUrl callbackUrl] :as opts :or {expires 3600}}]
  (let [auth (create-auth opts)
        ^StringMap sm (StringMap.)]
    (doseq [[k v] opts]
      (.put sm (name k) v))
    (.uploadToken auth bucket nil expires sm true)))

(defn- map->string-map
  [m]
  (let [sm (StringMap.)]
    (doseq [[k v] m]
      (.put sm (name k) v))
    sm))

(defonce bm-cfg (Configuration. (Zone/zone0)))

(defn convert-qiniu-ex
  [^QiniuException qe]
  (let [resp (.response qe)]
    {:status (.statusCode resp)
     :response (.bodyString resp)
     :exception qe}))

(defmacro resolve-qiniu-ex [& body]
  `(try
     (do
       ~@body)
     (catch QiniuException qe#
       (let [res# (convert-qiniu-ex qe#)]
         (if @throw-exception-atom?
           (throw (ex-info "Request faild." res#))
           res#)))))

(def default-return
  (constantly {:status 200 :ok true}))

(defn- ^BucketManager bucket-manager [opts]
  (BucketManager. (create-auth opts) bm-cfg))

(defn- ^UploadManager upload-manager [ops]
  (UploadManager. bm-cfg))

(defn upload
  "Upload a file to qiniu storage by token and key.
  The file should can be convert into InputStream by
     clojure.java.io/input-stream
  function."
  [^String token ^String key file & {:keys [mimeType params] :as opts}]
  (let [^UploadManager um (upload-manager opts)
        ^InputStream is (io/input-stream file)]
    (resolve-qiniu-ex
     (-> um
         (.put is key token (map->string-map params) mimeType)
         ((fn [response]
            (let [dpr (.fromJson (Gson.) (.bodyString response) DefaultPutRet)]
              {:status 200
               :ok true
               :key (.key dpr)
               :hash (.hash dpr)})))))))

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
  (format "http://%s/%s" domain (URLEncoder/encode key "utf-8")))

(defn private-download-url
  "Create a download url for public file."
  [domain key & {:keys [expires access-key secret-key] :as opts}]
  (let [auth (create-auth opts)
        ^String base-url (public-download-url domain key)]
    (.privateDownloadUrl auth base-url expires)))

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

(defn- file-info->map [^FileInfo fi]
  {:ok true
   :status 200
   :key (.key fi)
   :hash (.hash fi)
   :fsize (.fsize fi)
   :putTime (.putTime fi)
   :mimeType (.mimeType fi)
   :endUser (.endUser fi)
   :type (.type fi)})

(defn key->array
  [key]
  (if (seq? key)
    (into-array key)
    (into-array [key])))

(defn stat
  "Stat a file."
  [bucket key & opts]
  (if-not batch-mode
    (resolve-qiniu-ex
     (->
      (bucket-manager (apply hash-map opts))
      (.stat bucket key)
      (file-info->map)))
    (do (set-batch-op :stat)
        (.addStatOps batch-entries bucket (key->array key)))))

(defn copy
  "Copy a file"
  [src-bucket src-key dst-bucket dst-key & opts]
  (if-not batch-mode
    (resolve-qiniu-ex
     (->
      (bucket-manager (apply hash-map opts))
      (.copy src-bucket src-key dst-bucket dst-key)
      (default-return)))
    (do (set-batch-op :copy)
        (.addCopyOp batch-entries src-bucket src-key dst-bucket dst-key))))

(defn move
  "Move a file"
  [src-bucket src-key dst-bucket dst-key & opts]
  (if-not batch-mode
    (resolve-qiniu-ex
     (->
      (bucket-manager (apply hash-map opts))
      (.move src-bucket src-key dst-bucket dst-key)
      (default-return)))
    (do (set-batch-op :move)
        (.addMoveOp batch-entries src-bucket src-key dst-bucket dst-key))))

(defn delete
  "Delete a file."
  [bucket key & opts]
  (if-not batch-mode
    (resolve-qiniu-ex
     (->
      (bucket-manager (apply hash-map opts))
      (.delete bucket key)
      (default-return)))
    (do (set-batch-op :delete)
        (.addDeleteOp batch-entries bucket (key->array key)))))

(defn exec-batch
  [^BucketManager bm ops op]
  (cond
    (#{:stat :copy :move :delete} op) (.batch bm ops)
    (nil? op) nil
    :default (throw (ex-info (str "Unknown op:" op) {:op op}))))

(defn- batch-op-data->map
  [^BatchOpData bod]
  (when bod
    {:fsize (.fsize bod)
     :hash (.hash bod)
     :mimeType (.mimeType bod)
     :putTime (.putTime bod)
     :error (.error bod)}))

(defn- is-ok?
  [code]
  (= 200 code))



(defn- convert-batchret [^Response ret op]
  (let [ok (.isOK ret)
        status (.statusCode ret)
        result (.jsonToObject ret (Class/forName "[Lcom.qiniu.storage.model.BatchStatus;"))]
    {:results
     (map (fn [x]
            (assoc (batch-op-data->map (.data x))
                   :status (.code x)
                   :ok (is-ok? (.code x)))) result)
     :ok ok
     :status status}))

(defn exec
  "Execute  batch operations.The entries must be the same type."
  [& {:keys [entries] :as opts}]
  (if batch-mode
    (try
      (-> (bucket-manager opts)
          (exec-batch (or entries batch-entries) @batch-op)
          (convert-batchret @batch-op))
      (catch QiniuException qe
        (convert-qiniu-ex qe))
      (finally
        (.clearOps batch-entries)
        (.remove batch-op)))
    (throw (ex-info "exec must be invoked in with-batch body."))))

(defmacro with-batch [ & body]
  `(binding [batch-entries (BucketManager$BatchOperations.)
             batch-mode true]
     (try
       ~@body
       (finally
         (.remove batch-op)))))


(defn- listitem->map [^FileInfo it]
  (when it
    {:key (.key it)
     :hash (.hash it)
     :fsize (.fsize it)
     :putTime (.putTime it)
     :mimeType (.mimeType it)
     :endUser (.endUser it)}))

(defn traversal-file-list-iterator
  [fli]
  (if (.hasNext fli)
    (concat
     (map listitem->map (.next fli))
     (if (.hasNext fli)
       (lazy-seq (traversal-file-list-iterator fli))
       (lazy-seq [])))
    (lazy-seq [])))

(defn bucket-file-seq
  "List files in a bucket. Returns a lazy sequence of result files."
  [bucket prefix & {:keys [limit bm delimiter] :or {limit 32} :as opts}]
  (let [bm (or bm (bucket-manager opts))]
    (when-let [fli
               (->
                bm
                (.createFileListIterator bucket prefix limit delimiter))]
      (traversal-file-list-iterator fli))))

(defonce valid-stat-items #{"apicall" "transfer" "space"})
(defonce qiniu-api-url "http://api.qiniu.com")
(def stats-grain "day")

(defn make-authorization [^String path]
  (let [^Auth auth (create-auth nil)]
    (str "QBox " (.sign auth (.getBytes path)))))

(defn http-request
  "Make authorized request to qiniu api."
  [path f & {:keys [domain body] :or {domain qiniu-api-url} :as opts}]
  (let [default {:socket-timeout 10000
                 :conn-timeout 5000
                 :content-type :json
                 :method :get
                 :form-params body
                 :url (str domain path)
                 :connection-manager @http-conn-manager
                 :throw-exceptions false
                 :as :json
                 :client-params
                 {"http.useragent" @USER-AGENT}
                 :headers
                 {"Authorization"
                  (make-authorization
                   (str path "\n"))}}
        {:keys [status body] :as resp} (http/request (merge default opts))]
    (if (= status 200)
      {:ok true
       :results (f body)}
      (if @throw-exception-atom?
        (throw (ex-info "Request failed." {:body body}))
        {:ok false :response body :status status :resp resp}))))

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

(defn domain-list
  "Get CDN domains of bucket"
  [bucket]
  (http-request (str "/v6/domain/list?" "tbl=" bucket) identity))

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

(defn bucket-info
  "Get bucket info."
  ([bucket]
   (http-request (format "/v2/bucketInfo?bucket=%s" bucket)
                 identity
                 :method :post
                 :domain "https://uc.qbox.me")))

(defn private-bucket
  "Set a bucket to be private."
  ([bucket]
   (private-bucket bucket true))
  ([bucket private?]
   (http-request (format "/private?bucket=%s&private=%d"
                         bucket
                         (if private?
                           1
                           0))
                 identity
                 :method :post
                 :domain "https://uc.qbox.me")))

(defn refresh-bucket-cdn [urls dirs]
  (http-request "/v2/tune/refresh" identity
                :domain "http://fusion.qiniuapi.com"
                :method :post
                :body {:urls urls
                       :dirs dirs}))

(defn publish-bucket
  "Publish bucket as public domain."
  [bucket domain]
  (http-request (str "/publish/" (String. ^bytes (Base64/encode (.getBytes ^String domain) Base64/URL_SAFE))  "/from/" bucket) identity
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
  (let [path (format "/pfop/?bucket=%s&key=%s&fops=%s&notifyURL=%s" bucket key fops notifyURL)
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

(defn set-custom-domain!
  "bind custom domain "
  [domain bucket cert key platform geo protocol]
  (let [result (http-request (str "/v2/domains/" domain) identity
                             :method :post
                             :domain "http://fusion.qiniuapi.com"
                             :body {:sourceType "qiniuBucket"
                                    :sourceQiniuBucket bucket
                                    :serverCrt  cert
                                    :serverKey key
                                    :platform platform
                                    :geoCover geo
                                    :protocol protocol})]
    (if (:ok result)
      (http-request (str "/v2/domains/" domain) identity
                    :domain "http://fusion.qiniuapi.com")
      result)))

(defn offline-custom-domain!
  "offline custom domain"
  [domain]
  (http-request (format "/v2/domains/%s/offline" domain) identity
                :method :post
                :domain "http://fusion.qiniuapi.com"))

(defn online-custom-domain!
  "online custom domain"
  [domain]
  (http-request (format "/v2/domains/%s/online" domain) identity
                :method :post
                :domain "http://fusion.qiniuapi.com"))

(defn delete-custom-domain
  "delete custom domain"
  [domain]
  (http-request (str "/v2/domains/v3/" domain) identity
                :method :delete
                :domain "http://fusion.qiniuapi.com"))
