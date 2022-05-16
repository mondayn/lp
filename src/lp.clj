(ns lp
  (:use [clojure.repl])
  (:require
   [reaver]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [postal.core] ; email
   [hiccup.core] ; html 
   [hiccup.table])
   )

(defn parse-page
  "for a given webpage, return a collection of maps"
  [page_num]
  (reaver/extract-from (reaver/parse (slurp
                                      (str (System/getenv "LP") page_num))) ".listing-card"
                       [:title :price :url]
                       ".title" reaver/text
                       ".price" reaver/text
                       "a" (reaver/attr :href)))

(def build-results
  "stack pages 1 thru 9"
  (mapcat #(parse-page %) (range 1 10)))


(def ds
  "data source"
  (jdbc/get-datasource {:dbtype "postgresql"
                        :dbname "util"
                        :host "localhost"
                        :user (System/getenv "USER")
                        :password (System/getenv "PASSWORD")}))

(defn string-to-decimal
  [s]
  (Float/parseFloat (clojure.string/replace s #"\$" "")))

;; (defn insert-record
;;   [m]
;;   (sql/insert! ds :lp_stg m))


(def data-for-stg
  "transform results into records"
  (vec (->>
        build-results
        (map #(update % :price string-to-decimal))
        (map #(mapv val %)))))

(def stg-and-get-current
  "drop and recreate staging table, load staging table, get current records"
  (with-open [conn (jdbc/get-connection ds)]

    (try (jdbc/execute! conn ["drop table lp_stg;"]) (catch Exception e 0))

    (jdbc/execute! conn ["create table lp_stg (title varchar (2000)
                            ,price decimal (18,2)
                            ,url varchar (2000)
                            ,created_at timestamp default now ());"])

    (sql/insert-multi! ds :lp_stg [:title :price :url] data-for-stg)

    (jdbc/execute! conn ["update lp
                        set end_date = current_timestamp
                        where title not in (select title from lp_stg);"])

    (jdbc/execute! conn ["insert into lp (title,price,url, start_date,end_date)
                         select title, price,url, now(), '2099-12-31 00:00:00'
                         from lp_stg
                         on conflict(title) do nothing;"])


    (jdbc/execute! conn ["select title as item_listing
,case when start_date = (select max (start_date) from lp) then 'Y' else ''end as new_listing
,price, url ,cast (start_date as date)
from lp
where end_date > current_date
order by new_listing desc, price asc"])))


(def make-html
  "make html table for emailing"
  (hiccup.core/html
   (hiccup.table/to-table1d
    (map vec stg-and-get-current)
    [0 "title" 2 "price" 1 "new" 4 "start" 3 "url"])))


(def format-table
  (clojure.string/replace
   "<style>
table {
  font-family: arial, sans-serif;
  border-collapse: collapse;
  width: 100%;
}
td, th {
  border: 1px solid #dddddd;
  text-align: left;
  padding: 8px;
}
tr:nth-child(even) {
  background-color: #dddddd;
}
</style>
", #"\n" ""))

(defn run [args]
  (println
   (postal.core/send-message {:host "smtp.gmail.com"
                              :user (System/getenv "EMAIL")
                              :pass (System/getenv "EMAILAPPPWD")
                              :port 587
                              :tls true}
                             {:from (System/getenv "EMAIL")
                              :to (System/getenv "EMAIL")
                              :subject "LP"
                              :body [{:type "text/html" :content
                                      (str format-table
                                           make-html)}]})))
