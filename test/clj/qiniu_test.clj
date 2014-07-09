(ns clj.qiniu-test
  (:import [com.qiniu.api.config Config]
           [java.io StringReader])
  (:require [clojure.test :refer :all]
            [clojure.string :as cstr]
            [clojure.java.io :as io]
            [clj.qiniu :refer :all]))

(def test-key "q35fOBw-ZP8KHFBpYWGlC1K158P4VK6ijOXdrLqC")
(def test-secret "7FI-G2Gb_0Ucppi5VUfKlHkmi5XcoSKtUMukcPaa")
(def test-bucket "clj-qiniu")

(defn uuid
  {:doc "Generate a salt,default is 48 bits."
   :tag String}
  ([] (uuid 48))
  ([n]
     (let [charseq (map char (concat
                              (range 48 58)     ; 0-9
                              (range 65 90)      ;A-Z
                              (range 97 123)))] ; a-z
       (cstr/join
        (repeatedly n #(rand-nth charseq))))))

(deftest test-config
  (testing "set-config!"
    (let [config (set-config! :access-key test-key :secret-key test-secret)]
      (is (= config Config))
      (is (= test-key Config/ACCESS_KEY))
      (is (= test-secret Config/SECRET_KEY))
      (is (= "Clojure/qiniu sdk" Config/USER_AGENT)))))

(deftest test-uptoken
  (testing "uptoken"
    (is (uptoken test-bucket :expires 10))
    (is (uptoken test-bucket :expires 10 :scope test-bucket))
    (is (uptoken test-bucket :expires 10 :scope test-bucket :callbackUrl "http://localhost"))
    (is (uptoken test-bucket :expires 10 :scope test-bucket :callbackUrl "http://localhost" :detectMime 1))
    (is (uptoken test-bucket :expires 10 :scope test-bucket  :callbackUrl "http://localhost" :detectMime 1 :insertOnly 1))
    (is (uptoken test-bucket :expires 10 :scope test-bucket  :callbackUrl "http://localhost" :detectMime 1 :insertOnly 1 :fsizeLimit (* 1024 1024)))
    (is (thrown? ClassCastException
                 (uptoken test-bucket :insertOnly true)))
    (is (thrown? ClassCastException
                 (uptoken test-bucket :fsizeLimit "1024")))))

(deftest test-upload
  (testing "upload"
    (let [k (uuid)
          ret (upload (uptoken test-bucket :detectMime 1) k (io/resource "clj/clojure.jpeg"))]
      (is (= k (:key ret)))
      (is (:hash ret))
      (is (= 200 (:status ret)))
      (is (= (str "http://clj-qiniu.qiniudn.com/" k) (public-download-url "clj-qiniu.qiniudn.com" k)))
      (is (.startsWith
           (private-download-url "clj-qiniu.qiniudn.com" k :expires 3600)
           (str "http://clj-qiniu.qiniudn.com/" k "?e="))))))

(deftest test-upload-bucket
  (let [k (uuid)
        ret (upload-bucket test-bucket k (io/resource "clj/clojure.jpeg"))]
    (is (= k (:key ret)))
    (is (:hash ret))
    (is (= 200 (:status ret)))
    (is (= (str "http://clj-qiniu.qiniudn.com/" k) (public-download-url "clj-qiniu.qiniudn.com" k)))))

(deftest test-stat
  (testing "stat a file"
    (let [{:keys [putTime mimeType hash fsize status]}
          (stat test-bucket "0vdgFysdny3BxUTEQYYGCLdMjzRpR1s34RFbbiB0SWUFmCGe")]
      (is (= 14048865740735254 putTime))
      (is (= "image/jpeg" mimeType))
      (is (= "FrIFzmntgHjL5SEFSP3t-WaWECPL" hash))
      (is (= 200 status))
      (is (= 16098 fsize)))))

(deftest test-copy
  (testing "Copy a file to other bucekt."
    (let [k (uuid)
          ret (copy test-bucket "0vdgFysdny3BxUTEQYYGCLdMjzRpR1s34RFbbiB0SWUFmCGe"
                    "clj-qiniu2" k)]
      (is (:ok ret))
      (is (= 200 (:status ret)))
      (let [{:keys [putTime mimeType hash fsize status]}
            (stat "clj-qiniu2" k)]
        (is (= "image/jpeg" mimeType))
        (is (= "FrIFzmntgHjL5SEFSP3t-WaWECPL" hash))
        (is (= 200 status))
        (is (= 16098 fsize))))))

(deftest test-move
  (testing "Move a file to other bucket."
    ))
