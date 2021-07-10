from urllib import request, error
from bs4 import BeautifulSoup
from datetime import datetime
from blitzdb import Document, FileBackend
from dictdiffer import diff
import subprocess
import string
import json
import re
import os
json_to_dump = []
integer = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def examine_element(elem):
    alpa = string.ascii_lowercase
    alpa = alpa + string.ascii_uppercase
    text = elem.text
    word = [letter for letter in text if letter in alpa]
    word = "".join(word).lower()
    number = [inti for inti in text if inti in integer]
    number = int("".join(number))
    return word, number


def str_to_date_type(date_string):
    date = list(date_string)
    for i, letter in enumerate(date):
        if letter in integer:
            date.remove(letter)
            date.insert(i, " " + letter)
            break
    date = "".join(date).replace(",", " ")
    datetime_object = datetime.strptime(date, '%b %d %Y')
    return datetime_object


def bring_all_together(list_with_three_dics):
    main_dic = list_with_three_dics.pop(0)
    to_iterate = list_with_three_dics
    for dics in to_iterate:
        for item in dics:
            main_dic[item] = dics[item]
    return main_dic


def check_if_error(error_list):
    for error in error_list:
        if error == {}:
            return True
    return False


def get_the_required_tags(urll, userid, path_end):
    try:
        html = request.urlopen(urll)
        dic_for_dataset = {}
        if html:
            try:
                read_in_syntax = BeautifulSoup(html.read(), "lxml")
                if path_end == "read":
                    rv_h1 = read_in_syntax.find("div", {"id": {"header"}}).find("a").text
                    dic_for_dataset["userid"] = userid
                    dic_for_dataset["username"] = rv_h1
                    rv_div_shelves_section = read_in_syntax.find("div", {"id": {"shelvesSection"}}).find_all("a", {"class": {"actionLinkLite", "selectedShelf"}})
                    for a_element in rv_div_shelves_section:
                        rv = examine_element(a_element)
                        dic_for_dataset[rv[0]] = rv[1]
                rv_book_title = read_in_syntax.find_all("td", {"class": {"field title"}})
                if rv_book_title:
                    books = []
                    for td_field_title in rv_book_title:
                        a_tag_book_title = td_field_title.find("a").text.replace("\n", "")
                        a_tag_book_title = re.sub(' +', ' ', a_tag_book_title)
                        books.append({"title": a_tag_book_title})
                    rv_tag_author = read_in_syntax.find_all("td", {"class": {"field author"}})
                    if rv_tag_author:
                        for elem in rv_tag_author:
                            author = elem.find("a").text
                            expend = books.pop(0)
                            expend["author"] = author
                            books.append(expend)
                    rv_rating_book = read_in_syntax.find_all("td", {"class": {"field avg_rating"}})
                    if rv_rating_book:
                        for elem in rv_rating_book:
                            rating = elem.find("div").text.replace("\n", "").replace(" ", "")
                            expend = books.pop(0)
                            expend["total_rating"] = float(rating)
                            books.append(expend)
                    rv_rating_book_indiv = read_in_syntax.find_all("span", {"class": {"staticStars notranslate"}})
                    if rv_rating_book_indiv:
                        for elem in rv_rating_book_indiv:
                            ind_rating = elem.find("span", {"class": {"staticStar p10", "staticStar p0"}}).text
                            expend = books.pop(0)
                            expend["ind_rating"] = ind_rating
                            books.append(expend)
                    rv_read_book_date = read_in_syntax.find_all("td", {"class": {"field date_read"}})
                    if rv_read_book_date:
                        for elem in rv_read_book_date:
                            read_b = elem.find("span", {"class": {"date_read_value", "greyText"}}).text.replace("\n", "").replace(" ", "")
                            expend = books.pop(0)
                            expend["read_book_date"] = read_b
                            books.append(expend)
                    rv_field_added = read_in_syntax.find_all("td", {"class": {"field date_added"}})
                    if rv_field_added:
                        for elem in rv_field_added:
                            date_add = elem.find("span").text.replace("\n", "").replace(" ", "")
                            # rv = str_to_date_type(date_add)
                            expend = books.pop(0)
                            expend["added"] = date_add
                            books.append(expend)
                    dic_for_dataset[path_end + "_books"] = books
                else:
                    dic_for_dataset[path_end + "_books"] = []

            except AttributeError as fehler:
                print("Der Fehler: {} ist aufgetreten bei UserID: {} aufgetreten. Ein Attribut wurde nicht gefunden".format(fehler, userid))

        return dic_for_dataset

    except error.URLError as fehler:
        print("Der Fehler: {} ist aufgetreten. Die aufgerufene Webseite existiert nicht.".format(fehler))


further_url = ["read", "currently-reading", "to-read"]
for user_id in range(68455650, 68455651):  # 68455650
    all_results = []
    for path_end in further_url:
        url = "https://www.goodreads.com/review/list?v=2&id={}&shelf={}".format(user_id, path_end)
        rv_url = get_the_required_tags(url, user_id, path_end)
        all_results.append(rv_url)
    if check_if_error(all_results):
        continue
    rv_to_send = bring_all_together(all_results)
    if "read_books" and "currently-reading_books" and "to-read_books" in rv_to_send:
        # print(rv_to_send)
        json_to_dump.append(rv_to_send)

for elem in json_to_dump:
    print(elem)
"""
backend = FileBackend("./db_goodread_accounts")


class GoodAccountUser(Document):
    pass


send_to_elastic = []
for new_entry in json_to_dump:
    books_one_user_account = GoodAccountUser(new_entry)
    try:
        old_dataset = backend.get(GoodAccountUser, {'userid': books_one_user_account.userid})
        if old_dataset:
            dic_old_set = {}
            for elem in old_dataset:
                if elem == "pk":
                    pass
                else:
                    dic_old_set[elem] = old_dataset[elem]
            if new_entry == dic_old_set:
                print("The datasets are to 100% identical. | This dataset can be skipped.")
                pass
            else:
                print("This datasets aren't to 100% in all attributes identical.")
                result = diff(new_entry, dic_old_set)
                dic_to_kibana = {'all': 0, 'read': 0, 'currentlyreading': 0, 'wanttoread': 0, 'read_books': [],
                                 'currently-reading_books': [], 'to-read_books': [], 'userid': new_entry['userid'],
                                 'username': new_entry['username']}
                for elem in result:
                    elem = list(elem)
                    if isinstance(elem[1], list):
                        dic_to_kibana[elem[1][0]].append(new_entry[elem[1][0]][elem[1][1]])
                    else:
                        key = elem[1]
                        value_new = elem[2][0]
                        value_old = elem[2][1]
                        dic_to_kibana[key] = value_new - value_old
                send_to_elastic.append(dic_to_kibana)
                print("Dataset is added to the list.")
                backend.delete(old_dataset)
                new_object_to_save = GoodAccountUser(new_entry)
                new_object_to_save.save(backend)
                backend.commit()

    except Document.MultipleDocumentsReturned as error:
        print("That shouldn't happen.")

    except Document.DoesNotExist as error:
        books_one_user_account.save(backend)
        backend.commit()
        del new_entry['pk']
        send_to_elastic.append(new_entry)
        print("The dataset was saved for the first time.")
print(send_to_elastic)

if len(send_to_elastic) == 0:
    print("There are no elements to resolve in json file. | Script is over.")
else:
    json.dump(send_to_elastic, open("data_for_kibana.json", "w"), sort_keys=True, ensure_ascii=False, indent=4)
    print("json file is now created.")
    if os.path.isfile("./data_for_kibana.json"):
        p = subprocess.Popen(["curl -u elastic -H 'Content-Type: application/x-ndjson' -XPOST '127.0.0.1:9200/goodreads_datasets/_bulk?pretty' --data-binary @data_for_kibana.json"], cwd="./")
        p.wait()             # curl -u elastic -H 'Content-Type: application/x-ndjson' -XPOST '<host>:<port>/shakespeare/_bulk?pretty' --data-binary @shakespeare.json
        print("Daten wurden in Kibana aktualisiert.")
    else:
        print("There are problems to create a json file. | That shouldn't happen!")
print("script ist over!")
"""
