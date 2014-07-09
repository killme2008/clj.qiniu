(ns clj.qiniu
  (:import [com.qiniu.api.config Config]
           [com.qiniu.api.rs PutPolicy URLUtils GetPolicy RSClient Entry
            EntryPath]
           [java.io InputStream File]
           [com.qiniu.api.io IoApi PutExtra PutRet]
           [com.qiniu.api.net CallRet]
           [com.qiniu.api.auth.digest Mac])
  (:require [clojure.java.io :as io]))

(defmacro ^:private set-value! [k v]
  `(when ~v
     (set! ~k ~v)))

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
  (let [mac (create-mac opts)
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

(defn callret->map [^CallRet ret]
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
  (let [mac (create-mac opts)
        ^String base-url (public-download-url domain key)
        ^GetPolicy gp (GetPolicy.)]
    (set-value! (.expires gp) expires)
    (.makeRequest gp base-url mac)))

(def ^{:private true :dynamic true} batch-mode false)
(def ^{:private true :dynamic true} batch-entries nil)

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

(defn stat
  "Stat file."
  [bucket key & {:keys [access-key secret-key]}]
  (if-not batch-mode
    (let [mac (create-mac access-key secret-key)]
      (-> mac
          (RSClient.)
          (.stat bucket key)
          (entry->map)))
    (conj entries (create-entry-path bucket key))))

(defn batch-stat [])

(defn copy []
  (if-not batch-mode
    (let )))

(defn move [])

(defn delete [])

(defmacro with-batch [])

(defn image-info [])

(defn image-exif [])

(defn image-view [])

(defn list-files [])
