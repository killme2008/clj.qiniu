(ns clj.qiniu-test
  (:import [java.io StringReader])
  (:require [clojure.test :refer :all]
            [clojure.string :as cstr]
            [clojure.java.io :as io]
            [clj.qiniu :refer :all]))

(def test-key "q35fOBw-ZP8KHFBpYWGlC1K158P4VK6ijOXdrLqC")
(def test-secret "7FI-G2Gb_0Ucppi5VUfKlHkmi5XcoSKtUMukcPaa")
(def test-bucket "clj-qiniu")

(use-fixtures :each (fn [f] (set-config! :access-key test-key :secret-key test-secret)
                      (f)))

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
      (is (= test-key (:ACCESS-KEY config)))
      (is (= test-secret (:SECRET-KEY config)))
      (is (= "Clojure/qiniu sdk 1.0" (:USER-AGENT config))))))

(deftest test-uptoken
  (testing "uptoken"
    (is (uptoken test-bucket :expires 10))
    (is (uptoken test-bucket :expires 10 :key "photos" :isPrefixalScope true))
    (is (uptoken test-bucket :expires 10 :key "photos/my.jpg" :callbackUrl "http://localhost"))
    (is (uptoken test-bucket :expires 10 :key "my.jpg" :callbackUrl "http://localhost" :detectMime 1))
    (is (uptoken test-bucket :expires 10 :key "my.jpg"  :callbackUrl "http://localhost" :detectMime 1 :insertOnly 1))
    (is (uptoken test-bucket :expires 10 :key test-bucket  :callbackUrl "http://localhost" :detectMime 1 :insertOnly 1 :fsizeLimit (* 1024 1024)))))

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
    (let [k1 (uuid)
          ret (upload-bucket test-bucket k1 (io/resource "clj/clojure.jpeg"))]
      (is (:ok ret))
      (let [k2 (uuid)
            ret (move test-bucket k1 "clj-qiniu2" k2)]
        (is (:ok ret))
        (is (nil? (:hash (stat test-bucket k1))))
        (let [{:keys [status mimeType fsize]} (stat "clj-qiniu2" k2)]
          (is (= "image/jpeg" mimeType))
          (is (= 16098 fsize))
          (is (= 200 status)))))))

(deftest test-delete
  (testing "delete"
    (let [k (uuid)
          ret (upload-bucket test-bucket k (io/resource "clj/clojure.jpeg"))]
      (is (= k (:key ret)))
      (is (= (:hash ret) (:hash (stat test-bucket k))))
      (let [ret (delete test-bucket k)]
        (is (:ok ret))
        (is (nil? (:hash (stat test-bucket k))))))))

(deftest test-bucket-file-seq
  (testing "bucket-file-seq"
    (let [s (bucket-file-seq test-bucket "cloud_code" :limit 1)]
      (is (= 3 (count s)))
      (is (every?
           (fn [f]
             (.startsWith (:key f) "cloud_code"))
           s)))))

(deftest test-batch
  (testing "batch different op"
    (is (thrown? RuntimeException
                 (with-batch
                   (stat test-bucket "cloud_code_1.png")
                   (delete test-bucket "clojure.png")
                   (exec)))))
  (testing "batch stat"
    (let [ret (with-batch
                (stat test-bucket "cloud_code_1.png")
                (stat test-bucket "cloud_code_2.png")
                (stat test-bucket "cloud_code_3.png")
                (exec))]
      (is (:ok ret))
      (is (= 3 (count (:results ret))))
      (is (every?
           (fn [f]
             (and (:hash f) (:putTime f) (= "image/png" (:mimeType f)) (:fsize f)))
           (:results ret)))))
  (testing "batch copy"
    (let [ret (with-batch
                (copy test-bucket "cloud_code_1.png" "clj-qiniu2" (uuid))
                (copy test-bucket "cloud_code_2.png" "clj-qiniu2" (uuid))
                (copy test-bucket "cloud_code_3.png" "clj-qiniu2" (uuid))
                (exec))]
      (is (:ok ret))
      (is (every?
           (fn [r]
             (:ok r))
           (:results ret)))
      (is (= 3 (count (:results ret))))))
  (testing "batch move and delete"
    (let [k1 (uuid)
          k2 (uuid)
          k3 (uuid)
          k4 (uuid)
          ret1 (upload-bucket test-bucket k1 (io/resource "clj/clojure.jpeg"))
          ret2 (upload-bucket test-bucket k2 (io/resource "clj/clojure.jpeg"))]
      (is (:ok ret1))
      (is (:ok ret2))
      (let [ret (with-batch
                  (move test-bucket k1 "clj-qiniu2" k3)
                  (move test-bucket k2 "clj-qiniu2" k4)
                  (exec))]
        (is (:ok ret))
        (is (= 2 (count (:results ret))))
        (is (every?
             (fn [r]
               (:ok r))
             (:results ret)))
        (let [ret (with-batch
                    (delete "clj-qiniu2" k3)
                    (delete "clj-qiniu2" k4)
                    (exec))]
          (is (:ok ret))
          (is (= 2 (count (:results ret))))
          (is (every?
               (fn [r]
                 (:ok r))
               (:results ret))))))))

(deftest test-bucket-stats
  (testing "bucket-stats"
    (let [ret (bucket-stats test-bucket "space" "20140701" "20140710")]
      (is (:ok ret))
      (is (:results ret))))
  (testing "bucket-monthly-stats"
    (let [ret (bucket-monthly-stats test-bucket "201407")]
      (is (:ok ret))
      (is (:results ret)))))

(deftest test-create-delete-bucket
  (testing "create and delete bucket"
    (is (:ok (mk-bucket "test")))
    (is (:ok (publish-bucket "test" "clj-qiniu3.qiniudn.com")))
    (is (:ok (remove-bucket "test")))))

(deftest test-domain-list
  (testing "get domain list of bucket"
    (let [ret (domain-list test-bucket)]
      (is (:ok ret))
      (is (:results ret)))))

(deftest test-bucket-info
  (testing "get bucket info"
    (let [ret (bucket-info test-bucket)]
      (is (:ok ret))
      (is (:results ret))))
  (testing "get non-existent bucket info"
    (let [ret (bucket-info "non-existent-bucket")]
      (is (= false (:ok ret)))
      (is (:response ret)))))

(deftest test-pfop
  (testing "pfop a video file."
    (let [resp (pfop "clj-qiniu"  (java.net.URLEncoder/encode "viva la vida.mp3")
                     "avthumb/m3u8/segtime/10/preset/audio_32k"
                     "http://cn-stg1.avoscloud.com/1.1/qiniu/persistentNotify"
                     :pipeline "dennis")]
      (is (:ok resp))
      (println "prefop-status:" (prefop-status (:results resp))))))
