package com.kadal.restcontroller;


import com.kadal.document.ElasticCustomer;
import com.kadal.elasticrepository.ECustomerRepository;
import com.kadal.entity.Customer;
import com.kadal.repository.CustomerRepository;
import com.kadal.response.Response;
import lombok.AllArgsConstructor;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

import static com.kadal.response.ResponseEnum.*;

@RestController
@AllArgsConstructor
@RequestMapping("/customer")
public class CustomerRestController {

    final ElasticsearchOperations elasticsearchOperations;
    final ECustomerRepository elasticCRepo;
    final CustomerRepository customerRepo;


    // add - entity
    @PostMapping("/add")
    public Response add(@RequestBody Customer customer) {

        Customer c = customerRepo.save(customer);
        // Eleastic Search insert
        ElasticCustomer ec = new ElasticCustomer();
        ec.setCid(c.getId());
        ec.setName(c.getName());
        ec.setEmail(c.getEmail());
        elasticCRepo.save(ec);

        return new Response(STATUS_TRUE.getResponse(), SUCCESSFUL_MESSAGE.getResponse(), c);
    }


    // list Elastic
    @GetMapping("/list/{page}")
    public Response list(@PathVariable int page) {

        Iterable<ElasticCustomer> ls = elasticCRepo.findAll();

        // Pageable
        Pageable pageable = PageRequest.of(page, 2);
        Page<ElasticCustomer> lsp = elasticCRepo.findAll(pageable);


        return new Response(STATUS_TRUE.getResponse(),
                SUCCESSFUL_MESSAGE.getResponse() + " ---> totalElements: " + lsp.getTotalElements() +
                        " totalPages: " + lsp.getTotalPages(),
                lsp.getContent());
    }


    // search data
    @GetMapping("/search/{q}/{page}")
    public Response search(@PathVariable String q, @PathVariable int page) {

        Pageable pageable = PageRequest.of(page, 2);
        Page<ElasticCustomer> lsp = elasticCRepo.searchEmailAndName(q, pageable);

        return new Response(STATUS_TRUE.getResponse(),
                SUCCESSFUL_MESSAGE.getResponse() + " ---> totalElements: " + lsp.getTotalElements() +
                        " totalPages: " + lsp.getTotalPages(),
                lsp.getContent());
    }


    @GetMapping("/emailCidSearch/{q}")
    public Response search(@PathVariable String q) {

        List<ElasticCustomer> ls = elasticCRepo.findByEmailContainsOrCidContains(q, q);
        return new Response(STATUS_TRUE.getResponse(), SUCCESSFUL_MESSAGE.getResponse(), ls);
    }

    @GetMapping("/globalSearch/{q}")
    public Response globalSearch(@PathVariable String q) {
        final NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(
                        QueryBuilders.matchQuery("name", q)
                                .fuzziness(Fuzziness.AUTO)
                                .prefixLength(2)
                                //.operator(Operator.AND) // AND olarak işaretlendirğinde sadece aranan ifadenin aynısını sonuç olarak gösterir
                                .minimumShouldMatch("50%") // kelimelerin aynen bulunmasını istediğimizde
                )
                .build();
        List<SearchHit<ElasticCustomer>> ls = elasticsearchOperations.search(query, ElasticCustomer.class).getSearchHits();

        return new Response(STATUS_TRUE.getResponse(), SUCCESSFUL_MESSAGE.getResponse(), ls);

    }


    @DeleteMapping("delete/{id}")
    public Response delete(@PathVariable("id") String id) {
        Response response = new Response();
        try {
            int cid = Integer.parseInt(id);

            Customer customer = customerRepo.findById(cid).get();
            customerRepo.deleteById(cid);

            Optional<ElasticCustomer> optC = elasticCRepo.findByCid(cid);
            if (optC.isPresent()) {
                ElasticCustomer elasticCustomer = optC.get();
                elasticCRepo.deleteById(elasticCustomer.getId());

                response.setStatus(STATUS_TRUE.getResponse());
                response.setMessage(SUCCESSFUL_MESSAGE.getResponse());
                response.setResult(elasticCustomer);
            } else {
                response.setStatus(STATUS_FALSE.getResponse());
                response.setMessage(ERROR_MESSAGE.getResponse() + ": " + id);
            }
        } catch (Exception ex) {
            response.setStatus(STATUS_FALSE.getResponse());
            response.setMessage(ERROR_MESSAGE.getResponse() + ": " + id);
        }

        return response;
    }


    //devam et
    @PutMapping("update")
    public Response update(@RequestBody Customer customer) {
        Response response = new Response();

        Optional<Customer> optC = customerRepo.findById(customer.getId());
        if (optC.isPresent()) {
            Customer c = optC.get();
            c.setName(customer.getName());
            c.setEmail(customer.getEmail());
            c.setPassword(customer.getPassword());
            c.setTelephone(customer.getTelephone());
            customerRepo.saveAndFlush(c);


            Optional<ElasticCustomer> optEc = elasticCRepo.findByCid(customer.getId());
            if (optEc.isPresent()) {
                ElasticCustomer elasticCustomer = optEc.get();
                elasticCustomer.setName(customer.getName());
                elasticCustomer.setEmail(customer.getEmail());

                elasticCRepo.deleteById(elasticCustomer.getId());
                elasticCRepo.save(elasticCustomer);

                response.setStatus(STATUS_TRUE.getResponse());
                response.setMessage(SUCCESSFUL_MESSAGE.getResponse());
                response.setResult(elasticCustomer);

            } else {
                response.setStatus(STATUS_FALSE.getResponse());
                response.setMessage(ERROR_MESSAGE.getResponse());
            }

        } else {
            response.setStatus(STATUS_FALSE.getResponse());
            response.setMessage(ERROR_MESSAGE.getResponse());
        }
        return response;
    }


}