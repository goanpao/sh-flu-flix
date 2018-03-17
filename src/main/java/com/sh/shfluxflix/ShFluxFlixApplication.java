package com.sh.shfluxflix;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ShFluxFlixApplication {

	@Bean
	CommandLineRunner demo(MovieRepository movieRepository) {
		return args -> {
			Stream.of("Aeon Flux", "Enter the Mono<Void>", "The fluxinator", "Silence Of Lambdas",
					"Reactive Mongos on Plane", "Y Tu Mono Tambien", "Attack of fluxxes", "Back to the future")
			.map(name -> new Movie(UUID.randomUUID().toString(), name, randomGenre()))
			.forEach(m -> movieRepository.save(m).subscribe(System.out::println));
		};
	}

	private String randomGenre() {
		String [] genres = "horror,romcom,documentary,action,drama".split(",");
		return genres[new Random().nextInt(genres.length)];
	}

	public static void main(String[] args) {
		SpringApplication.run(ShFluxFlixApplication.class, args);
	}

}

@Service
class MovieService {
	private final MovieRepository movieRepository;
	
	public Flux<MovieEvent> streamStreams(Movie movie) {
		Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
		Flux<MovieEvent> events = Flux.fromStream(Stream.generate(() -> new MovieEvent(movie, new Date(), randomUser())));
		
		return Flux.zip(interval, events).map(x -> x.getT2());
	}
	
	private String randomUser() {
		String [] users = "Savio, Swathi, Sunil, Sumit, Priya, Laxmi".split(",");
		return users[new Random().nextInt(users.length)];
	}

	public MovieService(MovieRepository movieRepository) {
		this.movieRepository = movieRepository;
	}
	
	public Mono<Movie> byId(String id) {
		return movieRepository.findById(id);
	}
	
	public Flux<Movie> all() {
		return movieRepository.findAll();
	}
}


@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
class MovieEvent {
	private Movie movie;
	private Date when;		
	private String user;
}

@RestController
@RequestMapping("/movies")
class MovieRestController {
	
	MovieService movieService;
	
	public MovieRestController(MovieService movieService) {
		this.movieService = movieService;
	}

	@GetMapping("/{id}")
	public Mono<Movie> byId(@PathVariable String id) {
		return movieService.byId(id);
	}
	
	@GetMapping
	public Flux<Movie> all() {
		return movieService.all();
	}
	
	@GetMapping(value="/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<MovieEvent> events(@PathVariable String id) {
		return movieService.byId(id)
				.flatMapMany(movie -> movieService.streamStreams(movie));
	}
}

interface MovieRepository extends ReactiveMongoRepository<Movie, String> {}

@Document
@NoArgsConstructor
@ToString
@AllArgsConstructor
@Data
class Movie {

	@Id
	private String id;

	private String title, genre;
}