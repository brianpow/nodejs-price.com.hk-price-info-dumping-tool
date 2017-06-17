'use strict'
var util = require("util");
var Promise = require("bluebird");
var fs = Promise.promisifyAll(require("fs"));
var request = require("requestretry");
var cheerio = require("cheerio");
var program = require('commander')
var flatten = require("array-flatten")
var toCSV = require("array-to-csv")

function increaseVerbosity(v, total) {
    return total + 1
}

function isRegExp(val) {
    let matches = val.match(/^\/(.+)\/(\w+)?$/)
    if (matches) {
        matches[2] = matches[2] || ""
        return matches.splice(1)
    }
    return null
}

function save(data) {
    if (program.verbose > 0)
        console.log("About to save the information to CSV file.")
    let flattened_data = flatten(data)

    let date = new Date().toISOString().slice(0, 19).replace(/:/g, "-").replace(/T/g, " ");
    let filename = "price.com.hk " + date + ".csv"
    let content = headers.concat(additionalHeaders).join(program.separator) + "\n" + flattened_data.join("\n")
    fs.writeFileAsync(filename, content).then(function() {
        console.log(util.format("%d records saved to %s!", flattened_data.length, filename))
    }).catch(errorHandler)
}

function parseList(categoryName, data) {
    if (program.verbose > 0)
        console.log("parseList")

    let jq = cheerio.load(data)
    var brands = jq("select[name=brand] option").map(function() {
        return jq(this).text().trim()
    }).get() || []
    brands.shift()

    var category = categoryName


    return jq("div.item").map(function() {
        let $$tr = jq("div.line-04 tr", this);

        let brand = "",
            model = ""
        let data = [
            jq("div.line-01", this).eq(0).text().trim(),
            jq("div.line-02", this).eq(0).text().trim(),
            jq("div.price-range", this).eq(0).find("span.text-price-number").eq(0).text().trim(),
            jq("div.price-range", this).eq(0).find("span.text-price-number").eq(1).text().trim(),
            jq("div.price-range").eq(0).find("span.product-prop > img").attr("title"),
            jq("div.price-range", this).eq(1).find("span.text-price-number").eq(0).text().trim(),
            jq("div.price-range", this).eq(1).find("span.text-price-number").eq(1).text().trim(),
            jq("div.price-range", this).eq(1).find("span.product-prop > img").attr("title")
        ]
        if (!jq("a.btn", this).length)
            return "";

        for (let b of brands) {
            if (data[0].indexOf(b) != -1) {
                brand = b
                model = data[0].substr(brand.length).trim()
                data[0] = model
                break
            }
        }

        if (program.brand && (typeof program.brand == "object" && !program.brand.test(brand) || brand.indexOf(program.brand) == -1))
            return "";

        if (program.model && (typeof program.model == "object" && !program.model.test(model) || model.indexOf(program.model) != -1))
            return "";

            if (program.verbose > 1)
                console.log(util.format("Parsing %s...", jq("div.line-01", this).eq(0).text().trim().substr(0, 30)))

        $$tr.each(function() {
            let header = jq("td", this).eq(0).text().split(":")[0].trim()
            if (additionalHeaders.indexOf(header) == -1)
                additionalHeaders.push(header)
        })

        for (let header of additionalHeaders) {
            data.push($$tr.filter(function() {
                return jq("td", this).eq(0).text().trim().indexOf(header) != -1
            }).map(function() {
                return jq("td", this).eq(1).text().trim()
            }).get().join(""))
        }

        return [category, brand].concat(data).join(program.separator).trim()
    }).filter(function() {
        return this != ""
    }).get()
}

var getProducts = function(url, name, data) {
    if (program.verbose > 0)
        console.log(util.format("Finding products from %s at %s.", name, url))
    let deferreds = []
    let jq = cheerio.load(data)

    jq("ul.pagination li").last().find("a").eq(0).filter(function() {
        return this.children[0].data.indexOf("下一頁") != -1
    }).each(function() {
        let url = domain + this.attribs['href']
        if (program.verbose > 1)
            console.log(util.format("Additional pages found at %s.", url))
        deferreds.push(
            baseRequest({
                url: url
            }).then(getProducts.bind(null, url, name)).catch(errorHandler)
        )
    })

    deferreds.push(Promise.resolve(data).then(parseList.bind(null, name)).catch(errorHandler))
    return Promise.all(deferreds);
}

var domain = "http://www.price.com.hk"

function main(min, max) {
    let deferreds = []
    baseRequest({
        url: domain
    }).then(function(res) {
        let $ = cheerio.load(res)
        let $a = $("a[href*='category.php?c=']")
        console.log(util.format("%s categories found!", $a.length))
        $a.each(function(i) {
            min = (typeof min != "undefined") ? min : 0
            max = (typeof max != "undefined") ? max : 99999
            if (i >= min && i <= max) {
                let url = domain + this.attribs.href
                let category = this.children[0].data

                if (!program.category || (program.category && (typeof program.category == "object" && (program.category.test(category) || program.category.test(url)) || (category.indexOf(program.category) != -1 || url.indexOf(program.category)) != -1))) {
                    if (program.verbose > 1)
                        console.log(util.format("Found category %s with url: %s", category, url))
                    deferreds.push(
                        baseRequest({
                            url: url
                        }).then(getProducts.bind(null, url, category)).catch(errorHandler)
                    )
                }
            }
        })
        console.log(util.format("%s categories left after filter!", deferreds.length))
        console.log("Begins to parse and download product lists and information, it can take very long time, be patient.")

        return Promise.all(deferreds).then(save, save)
    }).catch(errorHandler)
}

function errorHandler(e) {
    console.error(e);
    if (program.exit)
        process.exit(1)
}

function banner() {
    console.log("price.com.hk price info dumping tool");
}

program
    .version('1.0.1')
    .option('-c, --category <keywords or regular expression>', 'Download only phone info with brands matching <keywords> or <regular expression>', "")
    .option('-b, --brand <keywords or regular expression>', 'Download only phone info with brands matching <keywords> or <regular expression>', "")
    .option('-d, --model <keywords or regular expression>', 'Download only phone info with models matching <keywords> or <regular expression>', "")
    .option('-s, --separator <separator>', 'separator of saved file [default: <TAB>]', "\t")
    .option('-m, --max-connection <max connection>', 'Maximum simultaneous HTTP connections, default is 2', parseInt, 2)
    .option('-t, --timeout <time in ms>', 'Timeout for each HTTP request, default is 60000ms', parseInt, 60000)
    .option('-r, --retry <count>', 'Retry if HTTP connections failed, default is 10', parseInt, 10)
    .option('-R, --retry-delay <time in ms>', 'Retry dealy if HTTP connections failed, default is 60000ms', parseInt, 60000)
    .option('-a, --user-agent <user agent>', 'User agent in HTTP request header, default is "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1')
    .option('-e, --exit', 'Exit on error, don\'t continue')
    .option('-v, --verbose', 'Be more verbose (max -vvv)', increaseVerbosity, 0)
    .parse(process.argv)

let re = isRegExp(program.category)
if (re)
    program.category = new RegExp(re[0], re[1])

re = isRegExp(program.brand)
if (re)
    program.brand = new RegExp(re[0], re[1])

re = isRegExp(program.model)
if (re)
    program.model = new RegExp(re[0], re[1])

var baseRequest = request.defaults({
    maxAttempts: program.retry,
    retryDelay: program.retryDelay,
    pool: {
        maxSockets: program.maxConnection
    },
    timeout: program.timeout,
    headers: {
        'User-Agent': program.userAgent
    },
    fullResponse: false
})

var headers = ["分類", "牌子", "牌子及型號", "簡介", "最低售價", "最高售價", "類型", "最低售價", "最高售價", "類型"]
var additionalHeaders = []
banner();
main();
