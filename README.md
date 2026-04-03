## My Stack Problems

> This project forked and has been modified from [A simple grey theme for Jekyll](https://github.com/liamsymonds/simplygrey-jekyll),
> and the search posts using [Super Search](https://github.com/chinchang/super-search)

### Demo
* [https://yellowpart.github.io](https://yellowpart.github.io)

#### Features

* Sitemap and XML Feed
* Pagination in homepage
* Posts under category
* Realtime Search Posts _(title & description)_ by query.
* Related Posts
* Highlight pre
* Next & Previous Post
* Disqus comment
* Projects page & Detail Project page
* Share on social media
* Google analytics
* HTML Minify _(Compress HTML)_ using [Jekyll Compress HTML](https://github.com/penibelst/jekyll-compress-html)

#### Screenshot

![Screenshot Post Page](https://raw.githubusercontent.com/yellowpart/yellowpart.github.io/master/static/img/screenshot-post-page.png  "Screenshot Post Page")

### Install & Configuration

1. Fork this repository
2. Edit site settings inside file of `_config.yml`
3. Edit your projects at file of `projects.md`, `_data/projects.json` and inside path of `_project/` _(for detail project)_.
4. Edit about yourself inside file of `about.md`

### How to Use?

**a. Add new Category**

All categories saved inside path of `category/`, you can see the existed categories.

**b. Add new Posts**

* All posts bassed on markdown syntax _(please googling)_. allowed extensions is `*.markdown` or `*.md`.
* This files can found at the path of `_posts/`.
* and the name of files are following `<date:%Y-%m-%d>-<slug>.<extension>`, for example:

```
2013-09-23-welcome-to-jekyll.md

# or

2013-09-23-welcome-to-jekyll.markdown
```

Inside the file of it,

```
---
layout: post                          # (require) default post layout
title: "Your Title"                   # (require) a string title
date: 2016-04-20 19:51:02 +0700       # (require) a post date
categories: [python, django]          # (custom) some categories, but makesure these categories already exists inside path of `category/`
tags: [foo, bar]                      # (custom) tags only for meta `property="article:tag"`
image: Broadcast_Mail.png             # (custom) image only for meta `property="og:image"`, save your image inside path of `static/img/_posts`
---

# your content post with markdown syntax goes here...
```


#### Installing in your local

```
bundle install
jekyll serve
```

### Contributing

Feel free to [open a bug](https://github.com/yellowpart/yellowpart.github.io/issues) or [contribute to code](https://github.com/yellowpart/yellowpart.github.io/pulls)!

### Contributing

Feel free to [open a bug](https://github.com/yellowpart/yellowpart.github.io/issues) or [contribute to code](https://github.com/yellowpart/yellowpart.github.io/pulls)!

### 배운점

오늘 실습을 하면서 파이썬의 기본 입출력 구조를 익혔다.  
`print()`를 사용하면 숫자와 문자를 출력할 수 있고, 문자는 따옴표로 감싸야 한다는 점을 확인했다.

또한 `print()` 안에서 `,`로 값을 나열하면 띄어쓰기가 들어가고, `+`로 연결하면 문자열이 붙어서 출력된다는 차이도 배웠다.

그리고 숫자 계산에서는 `**`가 제곱, `//`가 몫, `%`가 나머지를 구하는 연산자라는 것도 실습을 통해 이해했다.

입력 부분에서는 `input()`을 사용해 사용자가 값을 입력할 수 있다는 것을 배웠고, 입력한 값이 변수에 저장되어 나중에 다시 사용할 수 있다는 점도 알게 되었다.

마지막으로, 출력 → 입력 → 변수 저장 → 순차 실행처럼 기본 개념을 차근차근 익히는 것이 중요하다는 것을 느꼈다.

